import pytest

from extraredis.base import ExtraRedisBase


@pytest.fixture(scope='module')
def extraredis():
    return ExtraRedisBase()


def test_add_prefix(extraredis):
    assert extraredis.addprefix(b'kvtable', b'0') == b'kvtable:0'


def test_mremoveprefix(extraredis):
    assert extraredis.mremoveprefix(b'kvtable', [b'kvtable:0', b'kvtable:1']) == [b'0', b'1']


def test_encode_list(extraredis):
    assert extraredis.encode_list(['a', 'b', 'c']) == [b'a', b'b', b'c']


def test_decode_list(extraredis):
    assert extraredis.decode_list([b'a', b'b', b'c']) == ['a', 'b', 'c']


def test_encode_dict(extraredis):
    assert extraredis.encode_dict({'a': 'b', 'c': 'd'}) == {b'a': b'b', b'c': b'd'}


def test_decode_dict(extraredis):
    assert extraredis.decode_dict({b'a': b'b', b'c': b'd'}) == {'a': 'b', 'c': 'd'}


def test_encode_dict_keys(extraredis):
    assert extraredis.encode_dict_keys({'a': 'b', 'c': 'd'}) == {b'a': 'b', b'c': 'd'}


def test_decode_dict_keys(extraredis):
    assert extraredis.decode_dict_keys({b'a': 'b', b'c': 'd'}) == {'a': 'b', 'c': 'd'}


def test_encode_dict_values(extraredis):
    assert extraredis.encode_dict_values({'a': 'b', 'c': 'd'}) == {'a': b'b', 'c': b'd'}


def test_decode_dict_values(extraredis):
    assert extraredis.decode_dict_values({'a': b'b', 'c': b'd'}) == {'a': 'b', 'c': 'd'}
