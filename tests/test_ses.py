from string import Template
import pytest

from fondat.aws import Client, Config
from fondat.aws.ses import EmailRecipient, ses_resource

pytestmark = pytest.mark.asyncio

config = Config(
    endpoint_url="http://localhost:4566",
    aws_access_key_id="id",
    aws_secret_access_key="secret",
    region_name="us-east-1",
)


@pytest.fixture(scope="function")
async def client():
    async with Client(service_name="ses", config=config) as client:
        yield client


async def test_send(client):

    response = await ses_resource(client=client).send(
        email_from=EmailRecipient(
            email_address="test@test.io", first_name="First", last_name="Last"
        ),
        email_to=EmailRecipient(
            email_address="test@test.io", first_name="First", last_name="Last"
        ),
        text_body=Template(
            "From: $test <$test> \
            Subject: $test subject \
            To: $test $test <$test> \
            Content-Type: text/plain; charset='us-ascii' \
            Content-Transfer-Encoding: 7bit \
            \
            Dear $test: \
            \
            This is a $test. \
            \
            Thank you, \
            $test"
        ),
        text_pram={"test": "test"},
    )

    assert response["HTTPStatusCode"] == 200
