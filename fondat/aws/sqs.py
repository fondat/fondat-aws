"""Fondat module for Amazon Simple Queue Service (SQS)."""

import logging

from collections.abc import Iterable
from fondat.aws import Client
from fondat.codec import String, get_codec
from fondat.error import InternalServerError
from fondat.resource import resource, mutation
from fondat.security import Policy
from typing import Annotated, Optional


_logger = logging.getLogger(__name__)


def queue_resource(
    *,
    client: Client,
    queue_url: str,
    message_type: type,
    policies: Iterable[Policy] = None,
):
    """
    Create SQS queue resource.

    Parameters:
    • client: S3 client object
    • queue_url: the URL of the SQS queue
    • message_type: type of value transmitted in each message
    • security: security policies to apply to all operations
    """

    if client.service_name != "sqs":
        raise TypeError("expecting SQS client")

    codec = get_codec(String, message_type)

    @resource
    class Queue:
        """SQS queue resource."""

        @mutation(policies=policies)
        async def send(self, message: message_type) -> None:
            """Send a message to the queue."""
            try:
                await client.send_message(QueueUrl=queue_url, MessageBody=codec.encode(message))
            except Exception as e:
                _logger.error(e)
                raise InternalServerError from e

        @mutation(policies=policies)
        async def receive(
            self,
            wait_time_seconds: Annotated[int, "number of seconds to wait for a message"] = 0,
        ) -> Optional[message_type]:
            """Return the next message from the queue."""
            response = await client.receive_message(
                QueueUrl=queue_url, WaitTimeSeconds=wait_time_seconds
            )
            if "Messages" not in response:
                return None
            try:
                result = codec.decode(response["Messages"][0]["Body"])
            except Exception as e:
                _logger.error(e)
                raise InternalServerError from e
            return result

    return Queue()
