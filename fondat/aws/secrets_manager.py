"""Fondat module for Amazon Secrets Manager."""

import logging

from collections.abc import Iterable
from fondat.aws import Client
from fondat.resource import operation, resource, mutation
from fondat.security import Policy
from botocore.exceptions import ClientError


_logger = logging.getLogger(__name__)


def secretsmanager_resource(
    client: Client,
    policies: Iterable[Policy] = None,
):
    """
    Create Secrets Manager resource.

    Parameters:
    • client: Secrets Manager client object
    • policies: security policies to apply to all operations
    """

    if client.service_name != "secretsmanager":
        raise TypeError("expecting Secrets Manager client")

    @resource
    class Secret:
        """The secret object."""

        def __init__(self, secret: str):
            self.secret = secret

        @operation(policies=policies)
        async def delete(self):
            """Delete the secret from list of secrets."""
            await client.delete_secret(SecretId=self.secret)

    @resource
    class Secrets:
        """Secrets list for Amazon Secrets Manager."""

        @operation(policies=policies)
        async def post(self, secret):
            """Add a secret to list of secrets for secrets manager"""
            await client.create_secret(Name=secret)

        def __getitem__(self, secret) -> Secret:
            return Secret(secret)

    @resource
    class SecretsManagerResource:
        """Amazon Secrets Manager resource."""

        @mutation(policies=policies)
        async def get_secret(self, secret_name: str):
            """
            Retrieve a secret from Secrets Manager.
            Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/secrets-manager.html

            Parameters:
            • secret_name: The name of the secret or secret Amazon Resource Names (ARNs).
            """

            try:
                get_secret_value_response = await client.get_secret_value(SecretId=secret_name)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    print("The requested secret " + secret_name + " was not found")
                elif e.response["Error"]["Code"] == "InvalidRequestException":
                    print("The request was invalid due to:", e)
                elif e.response["Error"]["Code"] == "InvalidParameterException":
                    print("The request had invalid params:", e)
                elif e.response["Error"]["Code"] == "DecryptionFailure":
                    print(
                        "The requested secret can't be decrypted using the provided KMS key:", e
                    )
                elif e.response["Error"]["Code"] == "InternalServiceError":
                    print("An error occurred on service side:", e)
            else:
                if "SecretString" in get_secret_value_response:
                    return get_secret_value_response["SecretString"]
                else:
                    return get_secret_value_response["SecretBinary"]

        secrets = Secrets()

    return SecretsManagerResource()
