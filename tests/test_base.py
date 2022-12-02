import pytest

from extraredis import util


def test_encode_list():
    assert util.encode_list(['a', 'b', 'c']) == [b'a', b'b', b'c']


def test_decode_list():
    assert util.decode_list([b'a', b'b', b'c']) == ['a', 'b', 'c']


def test_encode_dict():
    assert util.encode_dict({'a': 'b', 'c': 'd'}) == {b'a': b'b', b'c': b'd'}


def test_decode_dict():
    assert util.decode_dict({b'a': b'b', b'c': b'd'}) == {'a': 'b', 'c': 'd'}


def test_encode_dict_keys():
    assert util.encode_dict_keys({'a': 'b', 'c': 'd'}) == {b'a': 'b', b'c': 'd'}


def test_decode_dict_keys():
    assert util.decode_dict_keys({b'a': 'b', b'c': 'd'}) == {'a': 'b', 'c': 'd'}


def test_encode_dict_values():
    assert util.encode_dict_values({'a': 'b', 'c': 'd'}) == {'a': b'b', 'c': b'd'}


def test_decode_dict_values():
    assert util.decode_dict_values({'a': b'b', 'c': b'd'}) == {'a': 'b', 'c': 'd'}
