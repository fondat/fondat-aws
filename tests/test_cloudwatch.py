import pytest

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fondat.aws import Client, Config
import fondat.aws.cloudwatch as cw
from fondat.aws.cloudwatch import CloudWatchMonitor
from fondat.error import NotFoundError
from fondat.monitoring import Measurement, Counter, Gauge, Absolute
from uuid import uuid4


pytestmark = pytest.mark.asyncio


config = Config(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="id",
    aws_secret_access_key="secret",
    region_name="us-east-1",
)


@pytest.fixture(scope="function")
async def client():
    async with Client(service_name="cloudwatch", config=config) as client:
        yield client


@pytest.fixture(scope="function")
async def metric_type(client):
    metric = cw.Metric(
        name="operation_requests_per_second",
        dimensions={"tenant_id": str(uuid4())},
        value=1234,
        timestamp=datetime.now(tz=timezone.utc),
        unit="Count",
    )
    yield metric


@pytest.fixture(scope="function")
async def measurement_counter(client):
    _now = lambda: datetime.now(tz=timezone.utc)
    _tags = {"name": "test"}
    measurement = Measurement(tags=_tags, timestamp=_now(), type="counter", value=1)
    yield measurement


@pytest.fixture(scope="function")
async def measurement_absolute(client):
    _now = lambda: datetime.now(tz=timezone.utc)
    _tags = {"name": "test"}
    measurement = Measurement(tags=_tags, timestamp=_now(), type="absolute", value=1)
    yield measurement


@pytest.fixture(scope="function")
async def measurement_gauge(client):
    _now = lambda: datetime.now(tz=timezone.utc)
    _tags = {"name": "test"}
    measurement = Measurement(tags=_tags, timestamp=_now(), type="gauge", value=1)
    yield measurement


async def test_put_metric(client, metric_type):
    resource = cw.cloudwatch_resource(client=client).namespace("Mperativ/Tripper")
    await resource.post(metrics=[metric_type])


async def test_put_counter(client, measurement_counter):
    cwm = CloudWatchMonitor(client=client, namespace="Mperativ/Tripper")
    await cwm.record(measurement_counter)


async def test_put_absolute(client, measurement_absolute):
    cwm = CloudWatchMonitor(client=client, namespace="Mperativ/Tripper")
    await cwm.record(measurement_absolute)


async def test_put_gauge(client, measurement_gauge):
    cwm = CloudWatchMonitor(client=client, namespace="Mperativ/Tripper")
    await cwm.record(measurement_gauge)
