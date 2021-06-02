from string import Template
import pytest

from fondat.aws import Client, Config
from fondat.aws.ses import EmailRecipient, ses_resource

pytestmark = pytest.mark.asyncio

# Run the following line before pytest
# aws ses verify-email-identity --email-address test@test.io --region us-east-1 --profile localstack --endpoint-url=http://localhost:4566
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

    test_str = """From: $test <$test>
Subject: $test subject
To: $test $test <$test>
Content-Type: text/plain; charset='us-ascii'
Content-Transfer-Encoding: 7bit

Dear $test:

This is a $test.

Thank you,
$test
"""

    response = await ses_resource(client=client).send(
        email_from="test@test.io",
        email_to="test@test.io",
        template=test_str,
        prams={"test": "test"},
    )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
