import pytest
import os
import itertools
import dotenv
from redis.asyncio import Redis


@pytest.fixture
def redis():
    dotenv.load_dotenv()
    return Redis(
        host=os.environ['REDIS_HOST'],
        port=os.environ['REDIS_PORT'],
        password=os.environ['REDIS_PASSWORD'],
    )


@pytest.mark.asyncio
async def test_prefix_mget(redis, state):
    await redis.set(b'foo:1', b'bar')
    await redis.set(b'foo:2', b'baz')
    assert await state.prefix_mget(b'foo') == {b'1': b'bar', b'2': b'baz'}


@pytest.mark.asyncio
async def test_prefix_mhset(redis, state):
    await state.prefix_mhset(b'baz', {b'1': b'SUCCESS', b'2': b'FAILED'}, itertools.repeat(b'status'))
    assert await redis.hgetall(b'baz:1') == {b'status': b'SUCCESS'}
    assert await redis.hgetall(b'baz:2') == {b'status': b'FAILED'}


@pytest.mark.asyncio
async def test_prefix_mhget(redis, state):
    await state.prefix_mhset(b'baz', {b'1': b'SUCCESS', b'2': b'FAILED'}, itertools.repeat(b'status'))
    assert await state.prefix_mhget(b'baz', itertools.repeat(b'status'), [b'1', b'2']) == {b'1': [b'SUCCESS'], b'2': [b'FAILED']}


# @pytest.mark.asyncio
# async def test_prefix_mhgetall(redis, state):
#     # pipe = redis.pipeline()
#     # pipe.hset('foo:1', 'bar', 'baz')
#     # pipe.hset('foo:2', 'bar', 'baz')
#     # await pipe.execute()
#     await state.mhset('foo', {'1': 'SUCCESS', '2': 'FAILED'}, itertools.repeat('status'))
#     assert await redis.hgetall('foo:1') == {b'status': b'SUCCESS'}



# # def test_remove_key_level():
#     keys = ['tasks:1', 'tasks:2', 'tasks:3']
#     assert State.remove_key_level(keys, 1) == ['1', '2', '3']
