import os

from extraredis.base import ExtraRedisBase

import redis.asyncio as redis_asyncio  # isort:skip
import redis as redis_sync  # isort:skip

redis_module = redis_asyncio


class ExtraRedisAsync(ExtraRedisBase):
    def __init__(self, redis: redis_module.Redis | None = None):
        self.redis = redis or redis_module.Redis(
            host=os.environ['REDIS_HOST'],
            port=os.environ['REDIS_PORT'],
            password=os.environ['REDIS_PASSWORD'],
        )

    async def maddprefix(self, prefix: bytes, keys: list[bytes] | None = None) -> list[bytes]:
        if keys is None:
            return await self.redis.keys(prefix + b':*')
        else:
            return [self.addprefix(prefix, k) for k in keys]

    async def mget(self, prefix: bytes, keys: list[bytes] | None = None) -> dict[bytes, bytes]:
        pkeys = await self.maddprefix(prefix, keys)
        values = await self.redis.mget(pkeys)
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    async def mset(self, prefix: bytes, mapping: dict[bytes, bytes]) -> None:
        mapping = {prefix + b':' + k: v for k, v in mapping.items()}
        await self.redis.mset(mapping)

    async def mhget_field(
        self,
        prefix: bytes,
        field: bytes,
        keys: list[bytes] | None = None,
    ) -> bytes | None:
        out = await self.mhget_fields(prefix, keys, [field])
        out = {k: v[field] for k, v in out.items()}
        return out

    async def mhget_fields(
        self,
        prefix: bytes,
        keys: list[bytes] | None = None,
        fields: list[bytes] | None = None,
    ) -> dict[bytes, dict[bytes, bytes]]:
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
        prefix: bytes,
        field: bytes,
        mapping: dict[bytes, bytes],
    ) -> None:
        pkeys = await self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, field, value)
        await pipe.execute()

    async def mhset_fields(
        self,
        prefix: bytes,
        mapping: dict[bytes, dict[bytes, bytes]],
    ) -> None:
        pkeys = await self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, mapping=value)
        await pipe.execute()
