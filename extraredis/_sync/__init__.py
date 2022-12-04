import os
from collections.abc import Iterable
from typing import AnyStr

import redis.asyncio as redis_sync  # isort:skip
import redis as redis_sync  # isort:skip

redis_module = redis_sync


class ExtraRedis:
    def __init__(
        self,
        redis: redis_module.Redis | None = None,
        **kwargs,
    ):
        self.decode_responses = kwargs.get('decode_responses', False)
        self.redis = redis or redis_module.Redis(
            host=os.environ['REDIS_HOST'],
            port=os.environ['REDIS_PORT'],
            password=os.environ['REDIS_PASSWORD'],
            **kwargs,
        )

    def addprefix(self, prefix: AnyStr, key: AnyStr) -> bytes:
        if self.decode_responses:
            return prefix + ':' + key
        return prefix + b':' + key

    def mremoveprefix(self, prefix: AnyStr, pkeys: list[AnyStr]) -> list[AnyStr]:
        if self.decode_responses:
            return [k.removeprefix(prefix + ':') for k in pkeys]
        return [k.removeprefix(prefix + b':') for k in pkeys]

    def maddprefix(self, prefix: AnyStr, keys: list[AnyStr] | None = None) -> list[AnyStr]:
        if keys is None:
            if self.decode_responses:
                suffix = ':*'
            else:
                suffix = b':*'
            return self.redis.keys(prefix + suffix)
        else:
            return [self.addprefix(prefix, k) for k in keys]

    def get(self, prefix: AnyStr, key: AnyStr) -> AnyStr | None:
        pkey = self.addprefix(prefix, key)
        return self.redis.get(pkey)

    def set(self, prefix: AnyStr, key: AnyStr, value: AnyStr) -> None:
        pkey = self.addprefix(prefix, key)
        self.redis.set(pkey, value)

    def delete(self, prefix: AnyStr, *keys: Iterable[AnyStr]) -> None:
        pkeys = self.maddprefix(prefix, keys)
        self.redis.delete(*pkeys)

    def mget(self, prefix: AnyStr, keys: Iterable[AnyStr] | None = None) -> dict[AnyStr, AnyStr]:
        pkeys = self.maddprefix(prefix, keys)
        values = self.redis.mget(pkeys)
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    def mset(self, prefix: AnyStr, mapping: dict[AnyStr, AnyStr]) -> None:
        if len(mapping) == 0:
            return
        if self.decode_responses:
            mapping = {prefix + ':' + k: v for k, v in mapping.items()}
        else:
            mapping = {prefix + b':' + k: v for k, v in mapping.items()}
        self.redis.mset(mapping)

    def hget_field(
        self,
        prefix: AnyStr,
        key: AnyStr,
        field: AnyStr,
    ) -> AnyStr | None:
        pkey = self.addprefix(prefix, key)
        return self.redis.hget(pkey, field)

    def hget_fields(
        self,
        prefix: AnyStr,
        key: AnyStr,
        fields: list[AnyStr] | None = None,
    ) -> dict[AnyStr, AnyStr]:
        pkey = self.addprefix(prefix, key)
        if fields is None:
            return self.redis.hgetall(pkey)
        else:
            values = self.redis.hmget(pkey, fields)
            return dict(zip(fields, values))

    def hset_field(
        self,
        prefix: AnyStr,
        key: AnyStr,
        field: AnyStr,
        value: AnyStr,
    ) -> None:
        pkey = self.addprefix(prefix, key)
        self.redis.hset(pkey, field, value)

    def hset_fields(
        self,
        prefix: AnyStr,
        key: AnyStr,
        mapping: dict[AnyStr, AnyStr],
    ) -> None:
        pkey = self.addprefix(prefix, key)
        self.redis.hset(pkey, mapping=mapping)

    def mhget_field(
        self,
        prefix: AnyStr,
        field: AnyStr,
        keys: list[AnyStr] | None = None,
    ) -> AnyStr | None:
        out = self.mhget_fields(prefix, keys, [field])
        out = {k: v[field] for k, v in out.items()}
        return out

    def mhget_fields(
        self,
        prefix: AnyStr,
        keys: list[AnyStr] | None = None,
        fields: list[AnyStr] | None = None,
    ) -> dict[AnyStr, dict[AnyStr, AnyStr]]:
        pkeys = self.maddprefix(prefix, keys)
        pipe = self.redis.pipeline()
        for key in pkeys:
            if fields is None:
                pipe.hgetall(key)
            else:
                pipe.hmget(key, fields)
        values = pipe.execute()
        if fields is not None:
            values = [dict(zip(fields, v)) for v in values]
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    def mhset_field(
        self,
        prefix: AnyStr,
        field: AnyStr,
        mapping: dict[AnyStr, AnyStr],
    ) -> None:
        pkeys = self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, field, value)
        pipe.execute()

    def mhset_fields(
        self,
        prefix: AnyStr,
        mapping: dict[AnyStr, dict[AnyStr, AnyStr]],
    ) -> None:
        pkeys = self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, mapping=value)
        pipe.execute()
