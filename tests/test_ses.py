import pytest

import asyncio
import fondat.error

from fondat.aws import Client, Config
from fondat.aws.ses import ses_resource


pytestmark = pytest.mark.asyncio


config = Config(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="id",
    aws_secret_access_key="secret",
    region_name="us-east-1",
)


verified_source = "source@test.io"


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def client():
    async with Client(service_name="ses", config=config) as client:
        yield client


@pytest.fixture(scope="module")
async def resource(client):
    yield ses_resource(client)


@pytest.fixture(scope="module", autouse=True)
async def setup_module(resource):
    await resource.identities.post(verified_source)
    yield
    await resource.identities[verified_source].delete()


async def test_send_raw_verified_identity(resource, setup_module):
    await resource.send_raw_email(verified_source, "destination@test.io", b"message")


async def test_send_unverified_identity(resource):
    with pytest.raises(fondat.error.BadRequestError):
        await resource.send_raw_email("unverified@test.io", "destination@test.io", b"message")


async def test_verify_delete(resource):
    await resource.identities.post("verify_delete@test.io")
    await resource.identities["verify_delete@test.io"].delete()
