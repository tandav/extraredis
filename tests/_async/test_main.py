import pytest
import pytest_asyncio

from extraredis import ExtraRedisAsync

import fakeredis.aioredis as fake_redis_async  # isort:skip
import fakeredis as fake_redis_sync  # isort:skip


fake_redis_module = fake_redis_async


pytest_mark_asyncio = pytest.mark.asyncio


def pytest_mark_sync(f):
    return f  # no-op decorator


@pytest.fixture
def redis():
    return fake_redis_module.FakeRedis()


@pytest.fixture
def extraredis(redis):
    return ExtraRedisAsync(redis)


@pytest_asyncio.fixture
async def kvtable(redis):
    pipe = redis.pipeline()
    for i in range(3):
        pipe.set(f'kvtable:{i}'.encode(), i)
    await pipe.execute()


@pytest_asyncio.fixture
async def khashtable(redis):
    pipe = redis.pipeline()
    for i in range(3):
        pipe.hset(f'khashtable:{i}', mapping={'a': i, 'b': i * 10, 'c': i * 100})
    await pipe.execute()


@pytest_mark_asyncio
async def test_set_get(redis):
    await redis.set(b'foo', b'bar')
    assert await redis.get(b'foo') == b'bar'


@pytest_mark_asyncio
async def test_get_from_kvtable(redis, kvtable):
    assert await redis.keys() == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert await redis.get(b'kvtable:1') == b'1'


@pytest_mark_asyncio
async def test_get_from_khashtable(redis, khashtable):
    assert await redis.keys() == [b'khashtable:0', b'khashtable:1', b'khashtable:2']
    assert await redis.hgetall(b'khashtable:1') == {b'a': b'1', b'b': b'10', b'c': b'100'}


@pytest_mark_asyncio
async def test_keys(redis, extraredis, kvtable):
    assert await redis.keys() == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert await extraredis.maddprefix(b'kvtable') == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert await extraredis.maddprefix(b'kvtable', [b'0', b'1']) == [b'kvtable:0', b'kvtable:1']


@pytest_mark_asyncio
async def test_mget(extraredis, kvtable):
    assert await extraredis.mget(b'kvtable') == {b'0': b'0', b'1': b'1', b'2': b'2'}
    assert await extraredis.mget(b'kvtable', [b'0', b'1']) == {b'0': b'0', b'1': b'1'}


@pytest_mark_asyncio
async def test_mset(extraredis):
    await extraredis.mset(b'kvtable', {b'3': b'3', b'4': b'4'})
    assert await extraredis.mget(b'kvtable') == {b'3': b'3', b'4': b'4'}


@pytest_mark_asyncio
async def test_mhget_field(extraredis, khashtable):
    assert await extraredis.mhget_field(b'khashtable', field=b'b') == {b'0': b'0', b'1': b'10', b'2': b'20'}
    assert await extraredis.mhget_field(b'khashtable', field=b'b', keys=[b'0', b'1']) == {b'0': b'0', b'1': b'10'}


@pytest_mark_asyncio
async def test_mhget_fields(extraredis, khashtable):
    assert await extraredis.mhget_fields(b'khashtable') == {
        b'0': {b'a': b'0', b'b': b'0', b'c': b'0'},
        b'1': {b'a': b'1', b'b': b'10', b'c': b'100'},
        b'2': {b'a': b'2', b'b': b'20', b'c': b'200'},
    }
    assert await extraredis.mhget_fields(b'khashtable', fields=[b'a', b'b']) == {
        b'0': {b'a': b'0', b'b': b'0'},
        b'1': {b'a': b'1', b'b': b'10'},
        b'2': {b'a': b'2', b'b': b'20'},
    }
    assert await extraredis.mhget_fields(b'khashtable', keys=[b'1']) == {b'1': {b'a': b'1', b'b': b'10', b'c': b'100'}}
    assert await extraredis.mhget_fields(b'khashtable', keys=[b'1'], fields=[b'a', b'b']) == {b'1': {b'a': b'1', b'b': b'10'}}


@pytest_mark_asyncio
async def test_mhset_field(extraredis, khashtable):
    await extraredis.mhset_field(b'khashtable', field=b'c', mapping={b'2': b'222', b'3': b'333'})
    assert await extraredis.mhget_field(b'khashtable', field=b'c', keys=[b'2', b'3']) == {b'2': b'222', b'3': b'333'}


@pytest_mark_asyncio
async def test_mhset_fields(extraredis, khashtable):
    await extraredis.mhset_fields(
        b'khashtable', mapping={
            b'2': {b'a': b'222', b'b': b'2222', b'c': b'22222'},
            b'3': {b'a': b'333', b'b': b'3333', b'c': b'33333'},
        },
    )
    assert await extraredis.mhget_fields(b'khashtable', keys=[b'2', b'3']) == {
        b'2': {b'a': b'222', b'b': b'2222', b'c': b'22222'},
        b'3': {b'a': b'333', b'b': b'3333', b'c': b'33333'},
    }

# @pytest.mark.asyncio
# async def test_prefix_mhset(redis, state):
#     await state.prefix_mhset(b'baz', {b'1': b'SUCCESS', b'2': b'FAILED'}, itertools.repeat(b'status'))
#     assert await redis.hgetall(b'baz:1') == {b'status': b'SUCCESS'}
#     assert await redis.hgetall(b'baz:2') == {b'status': b'FAILED'}


# @pytest.mark.asyncio
# async def test_prefix_mhget(redis, state):
#     await state.prefix_mhset(b'baz', {b'1': b'SUCCESS', b'2': b'FAILED'}, itertools.repeat(b'status'))
#     assert await state.prefix_mhget(b'baz', itertools.repeat(b'status'), [b'1', b'2']) == {b'1': [b'SUCCESS'], b'2': [b'FAILED']}


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
