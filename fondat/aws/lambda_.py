"""Fondat module for AWS Lambda."""

import asyncio
import fondat.context
import fondat.http

from base64 import b64encode, b64decode
from collections.abc import Awaitable
from fondat.error import InternalServerError
from fondat.stream import BytesStream


def async_function(coroutine):
    """Return an AWS Lambda function that invokes an asynchronous coroutine function."""

    def function(event, context):
        return asyncio.run(coroutine(event, context))

    return function


def http_function(handler):
    """Return an AWS Lambda function to invoke a Fondat HTTP request handler."""

    async def handle(event, context):
        if event["version"] != "2.0":
            raise InternalServerError("expecting payload version: 2.0")
        with fondat.context.push(
            {
                "context": "fondat.aws.lambda.http",
                "lambda_event": event,
                "lambda_context": context,
            }
        ):
            request = fondat.http.Request()
            http = event["requestContext"]["http"]
            protocol, version = http["protocol"].split("/", 1)
            if protocol != "HTTP":
                raise InternalServerError("expecting HTTP protocol")
            request.method = http["method"]
            request.path = http["path"]
            request.version = version
            for key, value in event["headers"].items():
                request.headers[key] = value
            for cookie in event.get("cookies", ()):
                request.cookies.load(cookie)
            for key, value in event.get("queryStringParameters", {}).items():
                request.query[key] = value
            body = event.get("body")
            if body:
                request.body = BytesStream(
                    b64decode(body) if event["isBase64Encoded"] else body.encode(),
                    request.headers.get("content-length"),
                )
            response = await handler(request)
            headers = response.headers
            return {
                "isBase64Encoded": True,
                "statusCode": response.status,
                "headers": {k: ", ".join(headers.getall(k)) for k in headers.keys()},
                "body": (
                    b64encode(b"".join([b async for b in response.body])).decode()
                    if response.body is not None
                    else ""
                ),
            }

    return async_function(handle)
