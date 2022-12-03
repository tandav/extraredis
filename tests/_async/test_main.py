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
def redis_decode():
    return fake_redis_module.FakeRedis(decode_responses=True)


@pytest.fixture
def extraredis(redis):
    return ExtraRedisAsync(redis)


@pytest.fixture
def extraredis_decode(redis_decode):
    return ExtraRedisAsync(redis_decode, decode_responses=True)


@pytest_asyncio.fixture
async def kvtable(redis, redis_decode):
    pipe = redis.pipeline()
    pipe_d = redis_decode.pipeline()
    for i in range(3):
        pipe.set(f'kvtable:{i}'.encode(), i)
        pipe_d.set(f'kvtable:{i}'.encode(), i)
    await pipe.execute()
    await pipe_d.execute()


@pytest_asyncio.fixture
async def khashtable(redis, redis_decode):
    pipe = redis.pipeline()
    pipe_d = redis_decode.pipeline()
    for i in range(3):
        pipe.hset(f'khashtable:{i}', mapping={'a': i, 'b': i * 10, 'c': i * 100})
        pipe_d.hset(f'khashtable:{i}', mapping={'a': i, 'b': i * 10, 'c': i * 100})
    await pipe.execute()
    await pipe_d.execute()


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
async def test_keys(extraredis, extraredis_decode, kvtable):
    assert await extraredis.maddprefix(b'kvtable') == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert await extraredis.maddprefix(b'kvtable', [b'0', b'1']) == [b'kvtable:0', b'kvtable:1']
    assert await extraredis_decode.maddprefix('kvtable') == ['kvtable:0', 'kvtable:1', 'kvtable:2']
    assert await extraredis_decode.maddprefix('kvtable', ['0', '1']) == ['kvtable:0', 'kvtable:1']


@pytest_mark_asyncio
async def test_mget(extraredis, extraredis_decode, kvtable):
    assert await extraredis.mget(b'kvtable') == {b'0': b'0', b'1': b'1', b'2': b'2'}
    assert await extraredis.mget(b'kvtable', [b'0', b'1']) == {b'0': b'0', b'1': b'1'}
    assert await extraredis_decode.mget('kvtable') == {'0': '0', '1': '1', '2': '2'}
    assert await extraredis_decode.mget('kvtable', ['0', '1']) == {'0': '0', '1': '1'}


@pytest_mark_asyncio
async def test_mset(extraredis, extraredis_decode):
    await extraredis.mset(b'kvtable', {})
    await extraredis.mset(b'kvtable', {b'3': b'3', b'4': b'4'})
    assert await extraredis.mget(b'kvtable') == {b'3': b'3', b'4': b'4'}
    await extraredis_decode.mset('kvtable', {})
    await extraredis_decode.mset('kvtable', {'3': '3', '4': '4'})
    assert await extraredis_decode.mget('kvtable') == {'3': '3', '4': '4'}


@pytest_mark_asyncio
async def test_hget_field(extraredis, extraredis_decode, khashtable):
    assert await extraredis.hget_field(b'khashtable', b'1', b'a') == b'1'
    assert await extraredis.hget_field(b'khashtable', b'1', b'b') == b'10'
    assert await extraredis.hget_field(b'khashtable', b'1', b'c') == b'100'
    assert await extraredis_decode.hget_field('khashtable', '1', 'a') == '1'
    assert await extraredis_decode.hget_field('khashtable', '1', 'b') == '10'
    assert await extraredis_decode.hget_field('khashtable', '1', 'c') == '100'


@pytest_mark_asyncio
async def test_hget_fields(extraredis, extraredis_decode, khashtable):
    assert await extraredis.hget_fields(b'khashtable', b'1', [b'a', b'b']) == {b'a': b'1', b'b': b'10'}
    assert await extraredis_decode.hget_fields('khashtable', '1', ['a', 'b']) == {'a': '1', 'b': '10'}


@pytest_mark_asyncio
async def test_hset_field(extraredis, extraredis_decode, khashtable):
    await extraredis.hset_field(b'khashtable', b'1', b'a', b'10')
    assert await extraredis.hget_field(b'khashtable', b'1', b'a') == b'10'
    await extraredis_decode.hset_field('khashtable', '1', 'a', '10')
    assert await extraredis_decode.hget_field('khashtable', '1', 'a') == '10'


@pytest_mark_asyncio
async def test_hset_fields(extraredis, extraredis_decode, khashtable):
    await extraredis.hset_fields(b'khashtable', b'1', {b'a': b'10', b'b': b'20'})
    assert await extraredis.hget_fields(b'khashtable', b'1', [b'a', b'b']) == {b'a': b'10', b'b': b'20'}
    await extraredis_decode.hset_fields('khashtable', '1', {'a': '10', 'b': '20'})
    assert await extraredis_decode.hget_fields('khashtable', '1', ['a', 'b']) == {'a': '10', 'b': '20'}


@pytest_mark_asyncio
async def test_mhget_field(extraredis, extraredis_decode, khashtable):
    assert await extraredis.mhget_field(b'khashtable', field=b'b') == {b'0': b'0', b'1': b'10', b'2': b'20'}
    assert await extraredis.mhget_field(b'khashtable', field=b'b', keys=[b'0', b'1']) == {b'0': b'0', b'1': b'10'}
    assert await extraredis_decode.mhget_field('khashtable', field='b') == {'0': '0', '1': '10', '2': '20'}
    assert await extraredis_decode.mhget_field('khashtable', field='b', keys=['0', '1']) == {'0': '0', '1': '10'}


@pytest_mark_asyncio
async def test_mhget_fields(extraredis, extraredis_decode, khashtable):
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

    assert await extraredis_decode.mhget_fields('khashtable') == {
        '0': {'a': '0', 'b': '0', 'c': '0'},
        '1': {'a': '1', 'b': '10', 'c': '100'},
        '2': {'a': '2', 'b': '20', 'c': '200'},
    }
    assert await extraredis_decode.mhget_fields('khashtable', fields=['a', 'b']) == {
        '0': {'a': '0', 'b': '0'},
        '1': {'a': '1', 'b': '10'},
        '2': {'a': '2', 'b': '20'},
    }
    assert await extraredis_decode.mhget_fields('khashtable', keys=['1']) == {'1': {'a': '1', 'b': '10', 'c': '100'}}
    assert await extraredis_decode.mhget_fields('khashtable', keys=['1'], fields=['a', 'b']) == {'1': {'a': '1', 'b': '10'}}


@pytest_mark_asyncio
async def test_mhset_field(extraredis, extraredis_decode, khashtable):
    await extraredis.mhset_field(b'khashtable', field=b'c', mapping={b'2': b'222', b'3': b'333'})
    assert await extraredis.mhget_field(b'khashtable', field=b'c', keys=[b'2', b'3']) == {b'2': b'222', b'3': b'333'}
    await extraredis_decode.mhset_field('khashtable', field='c', mapping={'2': '222', '3': '333'})
    assert await extraredis_decode.mhget_field('khashtable', field='c', keys=['2', '3']) == {'2': '222', '3': '333'}


@pytest_mark_asyncio
async def test_mhset_fields(extraredis, extraredis_decode, khashtable):
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
    await extraredis_decode.mhset_fields(
        'khashtable', mapping={
            '2': {'a': '222', 'b': '2222', 'c': '22222'},
            '3': {'a': '333', 'b': '3333', 'c': '33333'},
        },
    )
    assert await extraredis_decode.mhget_fields('khashtable', keys=['2', '3']) == {
        '2': {'a': '222', 'b': '2222', 'c': '22222'},
        '3': {'a': '333', 'b': '3333', 'c': '33333'},
    }
