__version__ = '0.0.1'
import os

import dotenv

import redis.asyncio as redis_asyncio  # isort:skip
import redis as redis_sync  # isort:skip

redis_module = redis_asyncio


class ExtraRedis:
    def __init__(self, redis: redis_module.Redis | None = None):
        dotenv.load_dotenv()
        self.redis = redis or redis_module.Redis(
            host=os.environ['REDIS_HOST'],
            port=os.environ['REDIS_PORT'],
            password=os.environ['REDIS_PASSWORD'],
        )

    @staticmethod
    def addprefix(prefix: bytes, key: bytes) -> bytes:
        return prefix + b':' + key

    async def maddprefix(self, prefix: bytes, keys: list[bytes] | None = None) -> list[bytes]:
        if keys is None:
            return await self.redis.keys(prefix + b':*')
        else:
            return [self.addprefix(prefix, k) for k in keys]

    def mremoveprefix(self, prefix: bytes, pkeys: list[bytes]) -> list[bytes]:
        return [k.removeprefix(prefix + b':') for k in pkeys]

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
