import fondat.http
import http
import multidict

from base64 import b64encode, b64decode
from fondat.aws.lambda_ import http_function
from fondat.http import AsBody
from fondat.resource import resource, operation
from typing import Annotated


def test_http_get():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    function = http_function(fondat.http.Application(Resource()))

    event = {
        "version": "2.0",
        "routeKey": "ANY /",
        "rawPath": "/",
        "rawQueryString": "",
        "headers": {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate",
            "content-length": "0",
            "host": "test.execute-api.region-1.amazonaws.com",
            "user-agent": "Test/1.0",
            "x-amzn-trace-id": "Root=1-23456789-abcdef0123456789abcdef01",
            "x-forwarded-for": "127.0.0.1",
            "x-forwarded-port": "443",
            "x-forwarded-proto": "https",
        },
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "1abc2de3f4",
            "domainName": "test.execute-api.region-1.amazonaws.com",
            "domainPrefix": "test",
            "http": {
                "method": "GET",
                "path": "/",
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": "Test/1.0",
            },
            "requestId": "AQIDBAUGB1w4XDkICQ==",
            "routeKey": "ANY /",
            "stage": "$default",
            "time": "01/Jan/2021:00:00:00 +0000",
            "timeEpoch": 1609488000000,
        },
        "isBase64Encoded": False,
    }

    response = function(event, None)
    headers = multidict.CIMultiDict(response["headers"])

    assert response["statusCode"] == http.HTTPStatus.OK.value
    assert headers["content-type"] == "text/plain; charset=UTF-8"
    assert headers["content-length"] == "3"
    assert b64decode(response["body"]) == b"str"


def test_http_post():
    @resource
    class Resource:
        @operation
        async def post(self, body: Annotated[str, AsBody]) -> str:
            return body

    function = http_function(fondat.http.Application(Resource()))

    body = "content goes here"

    event = {
        "version": "2.0",
        "routeKey": "ANY /",
        "rawPath": "/",
        "rawQueryString": "",
        "headers": {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate",
            "content-length": str(len(body)),
            "host": "test.execute-api.region-1.amazonaws.com",
            "user-agent": "Test/1.0",
            "x-amzn-trace-id": "Root=1-23456789-abcdef0123456789abcdef01",
            "x-forwarded-for": "127.0.0.1",
            "x-forwarded-port": "443",
            "x-forwarded-proto": "https",
        },
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "1abc2de3f4",
            "domainName": "test.execute-api.region-1.amazonaws.com",
            "domainPrefix": "test",
            "http": {
                "method": "POST",
                "path": "/",
                "protocol": "HTTP/1.1",
                "sourceIp": "127.0.0.1",
                "userAgent": "Test/1.0",
            },
            "requestId": "AQIDBAUGB1w4XDkICQ==",
            "routeKey": "ANY /",
            "stage": "$default",
            "time": "01/Jan/2021:00:00:00 +0000",
            "timeEpoch": 1609488000000,
        },
        "isBase64Encoded": True,
        "body": b64encode(body.encode()),
    }

    response = function(event, None)
    headers = multidict.CIMultiDict(response["headers"])

    assert response["statusCode"] == http.HTTPStatus.OK.value
    assert headers["content-type"] == "text/plain; charset=UTF-8"
    assert headers["content-length"] == str(len(body))
    assert b64decode(response["body"]).decode() == body
