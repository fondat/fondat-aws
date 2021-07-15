"""Fondat module for Amazon Secrets Manager."""

import logging

from collections.abc import Iterable
from typing import Optional
from fondat.aws import Client, wrap_client_error
from fondat.resource import operation, resource, mutation
from fondat.security import Policy
from fondat.data import datacls

_logger = logging.getLogger(__name__)


@datacls
class Secret:
    """
    Secret dataclass.
    Attributes:
    • ARN: count of measured values
    • Name: sum of all measured values
    • VersionId: minimum measured value
    """

    ARN: str
    Name: str
    SecretString: Optional[str]
    SecretBinary: Optional[bytes]


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
    class SecretsResource:
        @mutation(policies=policies)
        async def create(self, secret, secret_string: Optional[str]):
            """Add a secret to secrets manager"""
            await client.create_secret(Name=secret, SecretString=secret_string)

        @operation(policies=policies)
        async def put(self, secret, secret_string: Optional[str]):
            """Update a secret in the secrets manager"""
            await client.put_secret_value(SecretId=secret, SecretString=secret_string)

        @operation(policies=policies)
        async def delete(self, secret):
            """Delete the secret."""
            await client.delete_secret(SecretId=secret)

    @resource
    class SecretsManagerResource:
        """Amazon Secrets Manager resource."""

        @mutation(policies=policies)
        async def get_secret(self, secret_name: str) -> Secret:
            """
            Retrieve a secret from Secrets Manager.

            Parameters:
            • secret_name: The name of the secret or secret Amazon Resource Names (ARNs).
            """

            with wrap_client_error():
                get_secret_value_response = await client.get_secret_value(SecretId=secret_name)

                if "SecretString" in get_secret_value_response:
                    return Secret(
                        ARN=get_secret_value_response["ARN"],
                        Name=get_secret_value_response["Name"],
                        SecretString=get_secret_value_response["SecretString"],
                    )
                else:
                    return Secret(
                        ARN=get_secret_value_response["ARN"],
                        Name=get_secret_value_response["Name"],
                        SecretBinary=get_secret_value_response["SecretBinary"],
                    )

        secretsresource = SecretsResource()

    return SecretsManagerResource()
