"""Fondat package for Amazon Web Services."""

import aiobotocore
import dataclasses

from botocore.exceptions import ClientError
from fondat.data import datacls
from fondat.error import error_for_status
from typing import Annotated as A, Optional


# fmt: off
@datacls
class Config:
    aws_access_key_id: A[Optional[str], "AWS access key ID"]
    aws_secret_access_key: A[Optional[str], "AWS secret access key"]
    aws_session_token: A[Optional[str], "AWS temporary session token"]
    endpoint_url: A[Optional[str], "URL to use for constructed client"]
    profile_name: A[Optional[str], "name of the AWS profile to use"]
    region_name: A[Optional[str], "region to use when creating connections"]
    verify: A[Optional[bool], "verify TLS certificates"]
# fmt: on


def _asdict(config):
    return {k: v for k, v in dataclasses.asdict(config).items() if v is not None}


class Client:
    """
    AWS client object.

    This class wraps an asynchronous AWS client object, and provides
    additional asynchronous open and close methods.

    Parameters:
    • service_name: the name of a service (example: "s3")
    • config: configuration object to initialize client
    • kwargs: client configuration arguments (overrides config)
    """

    def __init__(self, service_name: str, config: Config = None, **kwargs):
        self.service_name = service_name
        self._kwargs = {**(_asdict(config) if config else {}), **kwargs}
        self._client = None

    async def open(self) -> None:
        if self._client:
            raise RuntimeError("Client is already open")
        session = aiobotocore.get_session()
        client = session.create_client(service_name=self.service_name, **self._kwargs)
        self._client = await client.__aenter__()

    async def close(self) -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None

    def __getattr__(self, name):
        if not self._client:
            raise RuntimeError("Client is not open")
        return getattr(self._client, name)

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


from contextlib import contextmanager


@contextmanager
def wrap_client_error():
    """Catch any raised ClientError and reraise as a Fondat error."""
    try:
        yield
    except ClientError as ce:
        status = ce.response["ResponseMetadata"]["HTTPStatusCode"]
        message = ce.response["Error"]["Message"]
        raise error_for_status(status)(message)
