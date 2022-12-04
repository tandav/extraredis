import os
from collections.abc import Iterable
from typing import AnyStr

import redis.asyncio as redis_asyncio  # isort:skip
import redis as redis_sync  # isort:skip

redis_module = redis_asyncio


class ExtraRedisAsync:
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

    async def maddprefix(self, prefix: AnyStr, keys: list[AnyStr] | None = None) -> list[AnyStr]:
        if keys is None:
            if self.decode_responses:
                suffix = ':*'
            else:
                suffix = b':*'
            return await self.redis.keys(prefix + suffix)
        else:
            return [self.addprefix(prefix, k) for k in keys]

    async def get(self, prefix: AnyStr, key: AnyStr) -> AnyStr | None:
        pkey = self.addprefix(prefix, key)
        return await self.redis.get(pkey)

    async def set(self, prefix: AnyStr, key: AnyStr, value: AnyStr) -> None:
        pkey = self.addprefix(prefix, key)
        await self.redis.set(pkey, value)

    async def delete(self, prefix: AnyStr, *keys: Iterable[AnyStr]) -> None:
        pkeys = await self.maddprefix(prefix, keys)
        await self.redis.delete(*pkeys)

    async def mget(self, prefix: AnyStr, keys: Iterable[AnyStr] | None = None) -> dict[AnyStr, AnyStr]:
        pkeys = await self.maddprefix(prefix, keys)
        values = await self.redis.mget(pkeys)
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    async def mset(self, prefix: AnyStr, mapping: dict[AnyStr, AnyStr]) -> None:
        if len(mapping) == 0:
            return
        if self.decode_responses:
            mapping = {prefix + ':' + k: v for k, v in mapping.items()}
        else:
            mapping = {prefix + b':' + k: v for k, v in mapping.items()}
        await self.redis.mset(mapping)

    async def hget_field(
        self,
        prefix: AnyStr,
        key: AnyStr,
        field: AnyStr,
    ) -> AnyStr | None:
        pkey = self.addprefix(prefix, key)
        return await self.redis.hget(pkey, field)

    async def hget_fields(
        self,
        prefix: AnyStr,
        key: AnyStr,
        fields: list[AnyStr] | None = None,
    ) -> dict[AnyStr, AnyStr]:
        pkey = self.addprefix(prefix, key)
        if fields is None:
            return await self.redis.hgetall(pkey)
        else:
            values = await self.redis.hmget(pkey, fields)
            return dict(zip(fields, values))

    async def hset_field(
        self,
        prefix: AnyStr,
        key: AnyStr,
        field: AnyStr,
        value: AnyStr,
    ) -> None:
        pkey = self.addprefix(prefix, key)
        await self.redis.hset(pkey, field, value)

    async def hset_fields(
        self,
        prefix: AnyStr,
        key: AnyStr,
        mapping: dict[AnyStr, AnyStr],
    ) -> None:
        pkey = self.addprefix(prefix, key)
        await self.redis.hset(pkey, mapping=mapping)

    async def mhget_field(
        self,
        prefix: AnyStr,
        field: AnyStr,
        keys: list[AnyStr] | None = None,
    ) -> AnyStr | None:
        out = await self.mhget_fields(prefix, keys, [field])
        out = {k: v[field] for k, v in out.items()}
        return out

    async def mhget_fields(
        self,
        prefix: AnyStr,
        keys: list[AnyStr] | None = None,
        fields: list[AnyStr] | None = None,
    ) -> dict[AnyStr, dict[AnyStr, AnyStr]]:
        pkeys = await self.maddprefix(prefix, keys)
        pipe = self.redis.pipeline()
        for key in pkeys:
            if fields is None:
                pipe.hgetall(key)
            else:
                pipe.hmget(key, fields)
        values = await pipe.execute()
        if fields is not None:
            values = [dict(zip(fields, v)) for v in values]
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    async def mhset_field(
        self,
        prefix: AnyStr,
        field: AnyStr,
        mapping: dict[AnyStr, AnyStr],
    ) -> None:
        pkeys = await self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, field, value)
        await pipe.execute()

    async def mhset_fields(
        self,
        prefix: AnyStr,
        mapping: dict[AnyStr, dict[AnyStr, AnyStr]],
    ) -> None:
        pkeys = await self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, mapping=value)
        await pipe.execute()
