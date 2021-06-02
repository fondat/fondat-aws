from email import message_from_string
from string import Template

import logging

from collections.abc import Iterable
from fondat.aws import Client
from fondat.resource import resource, mutation
from fondat.security import Policy
from fondat.data import datacls
from email.utils import formataddr

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
            email_from: str,
            email_to: str,
            template: str,
            prams: dict,
        ):

            msg = message_from_string(Template(template).safe_substitute(prams))

            return await client.send_raw_email(
                Source=email_from,
                Destinations=[email_to],
                RawMessage={"Data": msg.get_payload()},
            )

    return EmailResource()
