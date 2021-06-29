import pytest

import asyncio

from fondat.aws import Client, Config
from fondat.aws.secrets_manager import secretsmanager_resource


pytestmark = pytest.mark.asyncio


config = Config(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="id",
    aws_secret_access_key="secret",
    region_name="us-east-1",
)


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def client():
    async with Client(service_name="secretsmanager", config=config) as client:
        yield client


@pytest.fixture(scope="module")
async def resource(client):
    yield secretsmanager_resource(client)


@pytest.fixture(scope="module", autouse=True)
async def setup_module(resource):
    await resource.secrets.post("test_secret")
    yield
    await resource.secrets["test_secret"].delete()


async def test_get_secret(resource, setup_module):
    await resource.get_secret("test_secret")
