import asyncio
import fondat.aws.client
import fondat.aws.s3
import pytest

from dataclasses import dataclass
from datetime import date, datetime
from fondat.aws.s3 import BucketResource, ObjectResource
from fondat.error import NotFoundError
from fondat.pagination import paginate
from fondat.stream import BytesStream, Reader, Stream
from pytest import fixture
from random import randbytes
from typing import TypedDict
from uuid import uuid4


@fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@fixture(scope="module")
async def client():
    async with fondat.aws.client.create_client("s3") as client:
        yield client


async def _empty(bucket):
    async with fondat.aws.client.create_client("s3") as client:
        while "Contents" in (response := await client.list_objects_v2(Bucket=bucket)):
            for item in response["Contents"]:
                await client.delete_object(Bucket=bucket, Key=item["Key"])


@fixture(scope="module")
async def _bucket(client):
    name = str(uuid4())
    await client.create_bucket(Bucket=name)
    try:
        yield name
    finally:
        await _empty(name)
        await client.delete_bucket(Bucket=name)


@fixture(scope="function")
async def bucket(_bucket):
    await _empty(_bucket)
    yield _bucket


async def test_crud(bucket):
    @dataclass
    class DC:
        id: str
        str_: str | None
        dict_: TypedDict("TD", {"a": int}) | None
        list_: list[int] | None
        set_: set[str] | None
        int_: int | None
        float_: float | None
        bool_: bool | None
        bytes_: bytes | None
        date_: date | None
        datetime_: datetime | None

    resource = BucketResource(name=bucket, value_type=DC)
    id = "7af8410d-ffa3-4598-bac8-9ac0e488c9df"
    value = DC(
        id=id,
        str_="string",
        dict_={"a": 1},
        list_=[1, 2, 3],
        set_={"foo", "bar"},
        int_=1,
        float_=2.3,
        bool_=True,
        bytes_=b"12345",
        date_=date.fromisoformat("2019-01-01"),
        datetime_=datetime.fromisoformat("2019-01-01T01:01:01+00:00"),
    )
    r = resource[id]
    await r.put(value)
    assert await r.get() == value
    value.dict_ = {"a": 2}
    value.list_ = [2, 3, 4]
    value.set_ = None
    value.int_ = 2
    value.float_ = 1.0
    value.bool_ = False
    value.bytes_ = None
    value.date_ = None
    value.datetime_ = None
    await r.put(value)
    assert await r.get() == value
    await r.delete()
    with pytest.raises(NotFoundError):
        await r.get()


async def test_pagination(bucket):
    resource = BucketResource(name=bucket, value_type=str)
    assert len([v async for v in paginate(resource.get)]) == 0
    count = 10
    for n in range(count):
        await resource[f"{n:04d}"].put("value")
    assert len([v async for v in paginate(resource.get)]) == count
    page = await resource.get(limit=count - 2)
    assert len(page.items) == count - 2
    page = await resource.get(cursor=page.cursor)
    assert len(page.items) == 2


async def test_prefix_suffix(bucket):
    resource = BucketResource(
        name=bucket,
        prefix="prefix/",
        suffix=".bin",
        value_type=str,
    )
    assert len([v async for v in paginate(resource.get)]) == 0
    count = 5
    for n in range(0, count):
        await resource[f"{n:04d}"].put(str(n))
    keys = [key async for key in paginate(resource.get)]
    assert len(keys) == count
    for key in keys:
        assert await resource[key].get() == str(int(key))


async def test_stream_basic(bucket):
    body = randbytes(fondat.aws.s3.CHUNK_SIZE // 2)
    object = ObjectResource(bucket=bucket, key="key", type=Stream)
    await object.put(BytesStream(body))  # should perform single put_object
    async with Reader(await object.get()) as reader:
        read = await reader.read()
    assert body == read


async def test_stream_multipart(bucket):
    body = randbytes(2 * fondat.aws.s3.CHUNK_SIZE + fondat.aws.s3.CHUNK_SIZE // 2)
    object = ObjectResource(bucket=bucket, key="key", type=Stream)
    await object.put(BytesStream(body))  # should upload as multipart
    async with Reader(await object.get()) as reader:
        read = await reader.read()
    assert body == read


async def test_stream_unknown_length(bucket):
    class UnknownLengthStream(Stream):
        def __init__(self, content):
            super().__init__(content_type="application/octet-stream")
            self._content = content

        async def __anext__(self):
            if self._content:
                result = self._content
                self._content = None
                return result
            raise StopAsyncIteration

        async def close(self):
            self._content = None

    body = randbytes(fondat.aws.s3.CHUNK_SIZE // 2)
    object = ObjectResource(bucket=bucket, key="key", type=Stream)
    await object.put(UnknownLengthStream(body))  # unknown length should upload as multipart
    async with Reader(await object.get()) as reader:
        read = await reader.read()
    assert body == read
