"""Fondat module for AWS CloudWatch."""

import logging

from collections import deque
from collections.abc import Iterable
from datetime import datetime
from fondat.aws import Client
from fondat.data import datacls
from fondat.resource import resource, operation, mutation
from fondat.security import Policy
from typing import Any, Literal, Optional, Union
from fondat.monitoring import Measurement, Counter, Gauge, Absolute


_logger = logging.getLogger(__name__)


Unit = Literal[
    "Seconds",
    "Microseconds",
    "Milliseconds",
    "Bytes",
    "Kilobytes",
    "Megabytes",
    "Gigabytes",
    "Terabytes",
    "Bits",
    "Kilobits",
    "Megabits",
    "Gigabits",
    "Terabits",
    "Percent",
    "Count",
    "Bytes/Second",
    "Kilobytes/Second",
    "Megabytes/Second",
    "Gigabytes/Second",
    "Terabytes/Second",
    "Bits/Second",
    "Kilobits/Second",
    "Megabits/Second",
    "Gigabits/Second",
    "Terabits/Second",
    "Count/Second",
]

Value = Union[int, float]

Values = dict[Union[int, float], Union[int, float]]  # value: count


@datacls
class Statistics:
    """
    Statistics measurement type.
    Attributes:
    • count: count of measured values
    • sum: sum of all measured values
    • minimum: minimum measured value
    • maximum: maximum measured value
    """

    count: Union[int, float]
    sum: Union[int, float]
    minimum: Union[int, float]
    maximum: Union[int, float]


@datacls
class Metric:
    """
    CloudWatch metric type.
    Attributes:
    • name: name of the CloudWatch metric
    • dimensions: name/value pair of categories for characteristics
    • value: value of measurement
    • timestamp: date and time of the measurement to record
    • unit: Optional[Unit]
    • resolution: granularity of the metric
    """

    name: str
    dimensions: dict[str, str]
    value: Union[Value, Values, Statistics]
    timestamp: datetime
    unit: Optional[Unit]
    resolution: Optional[int]


def cloudwatch_resource(
    *,
    client: Client,
    policies: Iterable[Policy] = None,
):
    """
    Create CloudWatch resource.
    Parameters:
    • client: CloudWatch client object
    • security: security requirements to apply to all operations
    """
    if client.service_name != "cloudwatch":
        raise TypeError("expecting cloudwatch client")

    @resource
    class NamespaceResource:
        """Create Namespace resource."""

        def __init__(self, name: str):
            self.name = name

        @operation(policies=policies)
        async def post(self, metrics: Iterable[Metric]):
            metrics = deque(metrics)
            data = []
            while metrics:
                metric = metrics.popleft()
                datum = {
                    "MetricName": metric.name,
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in metric.dimensions.items()
                    ],
                    "Timestamp": metric.timestamp,
                }
                if metric.unit:
                    datum["Unit"] = metric.unit
                if metric.resolution:
                    datum["Resolution"] = metric.resolution
                if isinstance(metric.value, (int, float)):
                    datum["Value"] = float(metric.value)
                elif isinstance(metric.value, dict):
                    datum["Values"] = [float(v) for v in metric.value.keys()]
                    datum["Counts"] = [float(v) for v in metric.value.values()]
                elif isinstance(metric.value, Statistics):
                    datum["StatisticValues"] = {
                        "SampleCount": float(metric.value.count),
                        "Sum": float(metric.value.sum),
                        "Minimum": float(metric.value.minimum),
                        "Maximum": float(metric.value.maximum),
                    }
                data.append(datum)
                if len(data) == 20 or not metrics:
                    await client.put_metric_data(Namespace=self.name, MetricData=data)

    @resource
    class CloudWatchResource:
        """Create CloudWatch resource."""

        def namespace(self, name: str) -> NamespaceResource:
            return NamespaceResource(name)

    return CloudWatchResource()


class CloudWatchMonitor:
    """
    A monitor that stores all recorded measurements in CloudWatch.
    """

    # future: collect metrics, send in batches

    def __init__(self, client: Client, namespace: str):
        self.resource = cloudwatch_resource(client=client).namespace(namespace)

    async def record(self, measurement: Measurement):
        """Record a measurement."""

        if measurement.type == "counter":
            m = Counter(timestamp=measurement.timestamp)
            m.record(measurement.value)
            metric = Metric(
                name=measurement.tags["name"],
                dimensions={"Name": measurement.type, "Value": str(m.value)},
                timestamp=measurement.timestamp,
                value=float(m.value),
                unit="Count",
            )
        elif measurement.type == "gauge":
            m = Gauge(timestamp=measurement.timestamp)
            m.record(measurement.value)
            metric = Metric(
                name=measurement.tags["name"],
                dimensions={"Name": measurement.type, "Value": str(measurement.value)},
                timestamp=measurement.timestamp,
                value=Statistics(
                    count=float(m.count),
                    sum=float(m.sum),
                    minimum=float(m.min),
                    maximum=float(m.max),
                ),
            )
        elif measurement.type == "absolute":
            m = Absolute(timestamp=measurement.timestamp)
            m.record(measurement.value)
            metric = Metric(
                name=measurement.tags["name"],
                dimensions={"Name": measurement.type, "Value": str(m.value)},
                timestamp=measurement.timestamp,
                value=float(m.value),
            )

        await self.resource.post(metrics=[metric])
