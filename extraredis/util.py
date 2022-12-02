def encode_list(values: list[str]) -> list[bytes]:
    return [value.encode() for value in values]


def decode_list(values: list[bytes]) -> list[str]:
    return [value.decode() for value in values]


def encode_dict(mapping: dict[str, str]) -> dict[bytes, bytes]:
    return {key.encode(): value.encode() for key, value in mapping.items()}


def decode_dict(mapping: dict[bytes, bytes]) -> dict[str, str]:
    return {key.decode(): value.decode() for key, value in mapping.items()}


def encode_dict_keys(mapping: dict[str, str]) -> dict[bytes, str]:
    return {key.encode(): value for key, value in mapping.items()}


def decode_dict_keys(mapping: dict[bytes, str]) -> dict[str, str]:
    return {key.decode(): value for key, value in mapping.items()}


def encode_dict_values(mapping: dict[str, str]) -> dict[str, bytes]:
    return {key: value.encode() for key, value in mapping.items()}


def decode_dict_values(mapping: dict[str, bytes]) -> dict[str, str]:
    return {key: value.decode() for key, value in mapping.items()}
