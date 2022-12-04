import pytest

from extraredis import ExtraRedis

import fakeredis.aioredis as fake_redis_sync  # isort:skip
import fakeredis as fake_redis_sync  # isort:skip


fake_redis_module = fake_redis_sync


pytest_mark_sync = pytest.mark.asyncio


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
    return ExtraRedis(redis)


@pytest.fixture
def extraredis_decode(redis_decode):
    return ExtraRedis(redis_decode, decode_responses=True)


@pytest.fixture
def kvtable(redis, redis_decode):
    pipe = redis.pipeline()
    pipe_d = redis_decode.pipeline()
    for i in range(3):
        pipe.set(f'kvtable:{i}'.encode(), i)
        pipe_d.set(f'kvtable:{i}'.encode(), i)
    pipe.execute()
    pipe_d.execute()


@pytest.fixture
def khashtable(redis, redis_decode):
    pipe = redis.pipeline()
    pipe_d = redis_decode.pipeline()
    for i in range(3):
        pipe.hset(f'khashtable:{i}', mapping={'a': i, 'b': i * 10, 'c': i * 100})
        pipe_d.hset(f'khashtable:{i}', mapping={'a': i, 'b': i * 10, 'c': i * 100})
    pipe.execute()
    pipe_d.execute()


@pytest_mark_sync
def test_set_get(redis):
    redis.set(b'foo', b'bar')
    assert redis.get(b'foo') == b'bar'


@pytest_mark_sync
def test_get_from_kvtable(redis, kvtable):
    assert redis.keys() == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert redis.get(b'kvtable:1') == b'1'


@pytest_mark_sync
def test_get_from_khashtable(redis, khashtable):
    assert redis.keys() == [b'khashtable:0', b'khashtable:1', b'khashtable:2']
    assert redis.hgetall(b'khashtable:1') == {b'a': b'1', b'b': b'10', b'c': b'100'}


@pytest_mark_sync
def test_keys(extraredis, extraredis_decode, kvtable):
    assert extraredis.maddprefix(b'kvtable') == [b'kvtable:0', b'kvtable:1', b'kvtable:2']
    assert extraredis.maddprefix(b'kvtable', [b'0', b'1']) == [b'kvtable:0', b'kvtable:1']
    assert extraredis_decode.maddprefix('kvtable') == ['kvtable:0', 'kvtable:1', 'kvtable:2']
    assert extraredis_decode.maddprefix('kvtable', ['0', '1']) == ['kvtable:0', 'kvtable:1']


@pytest_mark_sync
def test_get(extraredis, extraredis_decode, kvtable):
    assert extraredis.get(b'kvtable', b'1') == b'1'
    assert extraredis.get(b'kvtable', b'7') is None
    assert extraredis_decode.get('kvtable', '1') == '1'
    assert extraredis_decode.get('kvtable', '7') is None


@pytest_mark_sync
def test_set(extraredis, extraredis_decode, kvtable):
    extraredis.set(b'kvtable', b'5', b'5')
    assert extraredis.get(b'kvtable', b'5') == b'5'
    extraredis_decode.set('kvtable', '5', '5')
    assert extraredis_decode.get('kvtable', '5') == '5'


@pytest_mark_sync
def test_delete(extraredis, extraredis_decode, kvtable):
    extraredis.delete(b'kvtable', b'1')
    assert extraredis.get(b'kvtable', b'1') is None
    extraredis.delete(b'kvtable', b'2', b'3')
    assert extraredis.get(b'kvtable', b'2') is None
    assert extraredis.get(b'kvtable', b'3') is None

    extraredis_decode.delete('kvtable', '1')
    assert extraredis_decode.get('kvtable', '1') is None
    extraredis_decode.delete('kvtable', '2', '3')
    assert extraredis_decode.get('kvtable', '2') is None
    assert extraredis_decode.get('kvtable', '3') is None


@pytest_mark_sync
def test_mget(extraredis, extraredis_decode, kvtable):
    assert extraredis.mget(b'kvtable') == {b'0': b'0', b'1': b'1', b'2': b'2'}
    assert extraredis.mget(b'kvtable', [b'0', b'1']) == {b'0': b'0', b'1': b'1'}
    assert extraredis.mget(b'kvtable', [b'7']) == {b'7': None}

    assert extraredis_decode.mget('kvtable') == {'0': '0', '1': '1', '2': '2'}
    assert extraredis_decode.mget('kvtable', ['0', '1']) == {'0': '0', '1': '1'}
    assert extraredis_decode.mget('kvtable', ['7']) == {'7': None}


@pytest_mark_sync
def test_mset(extraredis, extraredis_decode):
    extraredis.mset(b'kvtable', {})
    extraredis.mset(b'kvtable', {b'3': b'3', b'4': b'4'})
    assert extraredis.mget(b'kvtable') == {b'3': b'3', b'4': b'4'}
    extraredis_decode.mset('kvtable', {})
    extraredis_decode.mset('kvtable', {'3': '3', '4': '4'})
    assert extraredis_decode.mget('kvtable') == {'3': '3', '4': '4'}


@pytest_mark_sync
def test_hget_field(extraredis, extraredis_decode, khashtable):
    assert extraredis.hget_field(b'khashtable', b'1', b'a') == b'1'
    assert extraredis.hget_field(b'khashtable', b'1', b'b') == b'10'
    assert extraredis.hget_field(b'khashtable', b'1', b'c') == b'100'
    assert extraredis.hget_field(b'khashtable', b'1', b'z') is None
    assert extraredis.hget_field(b'khashtable', b'7', b'a') is None

    assert extraredis_decode.hget_field('khashtable', '1', 'a') == '1'
    assert extraredis_decode.hget_field('khashtable', '1', 'b') == '10'
    assert extraredis_decode.hget_field('khashtable', '1', 'c') == '100'
    assert extraredis_decode.hget_field('khashtable', '1', 'z') is None
    assert extraredis_decode.hget_field('khashtable', '7', 'a') is None


@pytest_mark_sync
def test_hget_fields(extraredis, extraredis_decode, khashtable):
    assert extraredis.hget_fields(b'khashtable', b'1', [b'a', b'b']) == {b'a': b'1', b'b': b'10'}
    assert extraredis.hget_fields(b'khashtable', b'1', [b'a', b'z']) == {b'a': b'1', b'z': None}
    assert extraredis.hget_fields(b'khashtable', b'7', [b'a', b'z']) == {b'a': None, b'z': None}
    assert extraredis.hget_fields(b'khashtable', b'1') == {b'a': b'1', b'b': b'10', b'c': b'100'}
    assert extraredis.hget_fields(b'khashtable', b'7') == {}
    assert extraredis_decode.hget_fields('khashtable', '1', ['a', 'b']) == {'a': '1', 'b': '10'}
    assert extraredis_decode.hget_fields('khashtable', '1', ['a', 'z']) == {'a': '1', 'z': None}
    assert extraredis_decode.hget_fields('khashtable', '7', ['a', 'z']) == {'a': None, 'z': None}
    assert extraredis_decode.hget_fields('khashtable', '1') == {'a': '1', 'b': '10', 'c': '100'}
    assert extraredis_decode.hget_fields('khashtable', '7') == {}


@pytest_mark_sync
def test_hset_field(extraredis, extraredis_decode, khashtable):
    extraredis.hset_field(b'khashtable', b'1', b'a', b'10')
    assert extraredis.hget_field(b'khashtable', b'1', b'a') == b'10'
    extraredis_decode.hset_field('khashtable', '1', 'a', '10')
    assert extraredis_decode.hget_field('khashtable', '1', 'a') == '10'


@pytest_mark_sync
def test_hset_fields(extraredis, extraredis_decode, khashtable):
    extraredis.hset_fields(b'khashtable', b'1', {b'a': b'10', b'b': b'20'})
    assert extraredis.hget_fields(b'khashtable', b'1', [b'a', b'b']) == {b'a': b'10', b'b': b'20'}
    extraredis_decode.hset_fields('khashtable', '1', {'a': '10', 'b': '20'})
    assert extraredis_decode.hget_fields('khashtable', '1', ['a', 'b']) == {'a': '10', 'b': '20'}


@pytest_mark_sync
def test_mhget_field(extraredis, extraredis_decode, khashtable):
    assert extraredis.mhget_field(b'khashtable', field=b'b') == {b'0': b'0', b'1': b'10', b'2': b'20'}
    assert extraredis.mhget_field(b'khashtable', field=b'b', keys=[b'0', b'1']) == {b'0': b'0', b'1': b'10'}
    assert extraredis.mhget_field(b'khashtable', field=b'b', keys=[b'7']) == {b'7': None}
    assert extraredis.mhget_field(b'khashtable', field=b'z') == {b'0': None, b'1': None, b'2': None}
    assert extraredis.mhget_field(b'khashtable', field=b'z', keys=[b'0', b'1']) == {b'0': None, b'1': None}

    assert extraredis.mhget_field(b'khashtable', field=b'z', keys=[b'7']) == {b'7': None}

    assert extraredis_decode.mhget_field('khashtable', field='b') == {'0': '0', '1': '10', '2': '20'}
    assert extraredis_decode.mhget_field('khashtable', field='b', keys=['0', '1']) == {'0': '0', '1': '10'}
    assert extraredis_decode.mhget_field('khashtable', field='b', keys=['7']) == {'7': None}
    assert extraredis_decode.mhget_field('khashtable', field='z') == {'0': None, '1': None, '2': None}
    assert extraredis_decode.mhget_field('khashtable', field='z', keys=['0', '1']) == {'0': None, '1': None}
    assert extraredis_decode.mhget_field('khashtable', field='z', keys=['7']) == {'7': None}


@pytest_mark_sync
def test_mhget_fields(extraredis, extraredis_decode, khashtable):
    assert extraredis.mhget_fields(b'khashtable') == {
        b'0': {b'a': b'0', b'b': b'0', b'c': b'0'},
        b'1': {b'a': b'1', b'b': b'10', b'c': b'100'},
        b'2': {b'a': b'2', b'b': b'20', b'c': b'200'},
    }
    assert extraredis.mhget_fields(b'khashtable', fields=[b'a', b'b']) == {
        b'0': {b'a': b'0', b'b': b'0'},
        b'1': {b'a': b'1', b'b': b'10'},
        b'2': {b'a': b'2', b'b': b'20'},
    }
    assert extraredis.mhget_fields(b'khashtable', fields=[b'b'], keys=[b'7']) == {
        b'7': {b'b': None},
    }
    assert extraredis.mhget_fields(b'khashtable', fields=[b'z']) == {
        b'0': {b'z': None},
        b'1': {b'z': None},
        b'2': {b'z': None},
    }
    assert extraredis.mhget_fields(b'khashtable', fields=[b'z'], keys=[b'0', b'1']) == {
        b'0': {b'z': None},
        b'1': {b'z': None},
    }
    assert extraredis.mhget_fields(b'khashtable', fields=[b'z'], keys=[b'7']) == {
        b'7': {b'z': None},
    }
    assert extraredis.mhget_fields(b'khashtable', keys=[b'1']) == {b'1': {b'a': b'1', b'b': b'10', b'c': b'100'}}
    assert extraredis.mhget_fields(b'khashtable', keys=[b'1'], fields=[b'a', b'b']) == {b'1': {b'a': b'1', b'b': b'10'}}

    assert extraredis_decode.mhget_fields('khashtable') == {
        '0': {'a': '0', 'b': '0', 'c': '0'},
        '1': {'a': '1', 'b': '10', 'c': '100'},
        '2': {'a': '2', 'b': '20', 'c': '200'},
    }
    assert extraredis_decode.mhget_fields('khashtable', fields=['a', 'b']) == {
        '0': {'a': '0', 'b': '0'},
        '1': {'a': '1', 'b': '10'},
        '2': {'a': '2', 'b': '20'},
    }
    assert extraredis_decode.mhget_fields('khashtable', fields=['b'], keys=['7']) == {
        '7': {'b': None},
    }
    assert extraredis_decode.mhget_fields('khashtable', fields=['z']) == {
        '0': {'z': None},
        '1': {'z': None},
        '2': {'z': None},
    }
    assert extraredis_decode.mhget_fields('khashtable', fields=['z'], keys=['0', '1']) == {
        '0': {'z': None},
        '1': {'z': None},
    }
    assert extraredis_decode.mhget_fields('khashtable', fields=['z'], keys=['7']) == {
        '7': {'z': None},
    }
    assert extraredis_decode.mhget_fields('khashtable', keys=['1']) == {'1': {'a': '1', 'b': '10', 'c': '100'}}
    assert extraredis_decode.mhget_fields('khashtable', keys=['1'], fields=['a', 'b']) == {'1': {'a': '1', 'b': '10'}}


@pytest_mark_sync
def test_mhset_field(extraredis, extraredis_decode, khashtable):
    extraredis.mhset_field(b'khashtable', field=b'c', mapping={b'2': b'222', b'3': b'333'})
    assert extraredis.mhget_field(b'khashtable', field=b'c', keys=[b'2', b'3']) == {b'2': b'222', b'3': b'333'}
    extraredis_decode.mhset_field('khashtable', field='c', mapping={'2': '222', '3': '333'})
    assert extraredis_decode.mhget_field('khashtable', field='c', keys=['2', '3']) == {'2': '222', '3': '333'}


@pytest_mark_sync
def test_mhset_fields(extraredis, extraredis_decode, khashtable):
    extraredis.mhset_fields(
        b'khashtable', mapping={
            b'2': {b'a': b'222', b'b': b'2222', b'c': b'22222'},
            b'3': {b'a': b'333', b'b': b'3333', b'c': b'33333'},
        },
    )
    assert extraredis.mhget_fields(b'khashtable', keys=[b'2', b'3']) == {
        b'2': {b'a': b'222', b'b': b'2222', b'c': b'22222'},
        b'3': {b'a': b'333', b'b': b'3333', b'c': b'33333'},
    }
    extraredis_decode.mhset_fields(
        'khashtable', mapping={
            '2': {'a': '222', 'b': '2222', 'c': '22222'},
            '3': {'a': '333', 'b': '3333', 'c': '33333'},
        },
    )
    assert extraredis_decode.mhget_fields('khashtable', keys=['2', '3']) == {
        '2': {'a': '222', 'b': '2222', 'c': '22222'},
        '3': {'a': '333', 'b': '3333', 'c': '33333'},
    }
