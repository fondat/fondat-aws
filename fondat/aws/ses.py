from email import message_from_string
from string import Template

import logging

from collections.abc import Iterable
from fondat.aws import Client
from fondat.resource import resource, mutation
from fondat.security import Policy
from fondat.data import datacls
from email.utils import formataddr
from typing import Dict, Union


_logger = logging.getLogger(__name__)


@datacls
class EmailRecipient:
    email_address: str
    first_name: str
    last_name: str

    def recipient_format(self) -> str:

        return formataddr((f"{self.first_name} {self.last_name}", self.email_address))


def ses_resource(
    *,
    client: Client,
    policies: Iterable[Policy] = None,
):
    """
    Create SES resource.

    Parameters:
    • client: SES client object
    • message_type: type of value transmitted in each message
    • security: security policies to apply to all operations
    """

    if client.service_name != "ses":
        raise TypeError("expecting SES client")

    @resource
    class EmailResource:
        @mutation(policies=policies)
        async def send(
            self,
            email_from: Union[str, EmailRecipient],
            email_to: Union[str, EmailRecipient],
            text_body: Template,
            text_pram: Dict[str, str],
        ):

            msg = msg = message_from_string(text_body.safe_substitute(text_pram))

            return await client.send_email(
                Destination={
                    "ToAddresses": [
                        email_to.recipient_format(),
                    ],
                },
                Message={
                    "Body": {
                        "Text": {
                            "Charset": msg.get_charsets()[0],
                            "Data": msg.get_payload(),
                        },
                    },
                    "Subject": {
                        "Charset": msg.get_charsets()[0],
                        "Data": msg["Subject"],
                    },
                },
                Source=email_from.recipient_format(),
            )

    return EmailResource()
