__version__ = '0.0.1'
import os

import dotenv
import redis as redis_sync
import redis.asyncio as redis_sync

# import redis.asyncio as redis_asyncio
# import redis as redis_sync
# from redis.asyncio import Redis
# Redis = redis_asyncio.Redis

# redis_module = redis_asyncio.Redis
redis_module = redis_sync
# redis_module = redis_sync


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

    def maddprefix(self, prefix: bytes, keys: list[bytes] | None = None) -> list[bytes]:
        if keys is None:
            return self.redis.keys(prefix + b':*')
        else:
            return [self.addprefix(prefix, k) for k in keys]

    def mremoveprefix(self, prefix: bytes, pkeys: list[bytes]) -> list[bytes]:
        return [k.removeprefix(prefix + b':') for k in pkeys]

    def mget(self, prefix: bytes, keys: list[bytes] | None = None) -> dict[bytes, bytes]:
        pkeys = self.maddprefix(prefix, keys)
        values = self.redis.mget(pkeys)
        if keys is None:
            keys = self.mremoveprefix(prefix, pkeys)
        return dict(zip(keys, values))

    def mset(self, prefix: bytes, mapping: dict[bytes, bytes]) -> None:
        mapping = {prefix + b':' + k: v for k, v in mapping.items()}
        self.redis.mset(mapping)

    def mhget_field(
        self,
        prefix: bytes,
        field: bytes,
        keys: list[bytes] | None = None,
    ) -> bytes | None:
        out = self.mhget_fields(prefix, keys, [field])
        out = {k: v[field] for k, v in out.items()}
        return out

    def mhget_fields(
        self,
        prefix: bytes,
        keys: list[bytes] | None = None,
        fields: list[bytes] | None = None,
    ) -> dict[bytes, dict[bytes, bytes]]:
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
        prefix: bytes,
        field: bytes,
        mapping: dict[bytes, bytes],
    ) -> None:
        pkeys = self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, field, value)
        pipe.execute()

    def mhset_fields(
        self,
        prefix: bytes,
        mapping: dict[bytes, dict[bytes, bytes]],
    ) -> None:
        pkeys = self.maddprefix(prefix, mapping.keys())
        pipe = self.redis.pipeline()
        for key, value in zip(pkeys, mapping.values()):
            pipe.hset(key, mapping=value)
        pipe.execute()

    # mhgetall(prefix: bytes, keys: list[bytes] | None = None) -> dict[bytes, bytes]:
    # mhgetfield(prefix: bytes, keys: list[bytes] | None = None) -> dict[bytes, bytes]:

    # @staticmethod
    # def remove_key_level(keys: list[str], level: int) -> list[str]:
    #     return [key.split(':')[level] for key in keys]
    #     return [key.split(':')[1] for key in keys]

    # @staticmethod
    # def remove_table_prefix(keys: list[bytes]) -> list[bytes]:
    #     return [key.split(b':')[1] for key in keys]

    # async def prefix_mget(self, prefix: bytes, keys: list[str] | None = None) -> dict[str, bytes]:
    #     if keys is None:
    #         keys = await self.redis.keys(prefix + b':*')
    #     values = await self.redis.mget(keys)
    #     keys = self.remove_table_prefix(keys)
    #     return dict(zip(keys, values))

    # async def prefix_mhgetall(self, prefix: str, keys: list[str] | None = None) -> dict[str, bytes]:
    #     keys = await self.redis.keys(f'{prefix}:*')
    #     pipe = self.redis.pipeline()
    #     for key in keys:
    #         pipe.hgetall(key)
    #     values = await pipe.execute()
    #     keys = [k.decode() for k in keys]
    #     return dict(zip(keys, values))

    # async def prefix_mhset(self, prefix: str, nv: dict[str, str], fields: list[str] | None = None):
    #     if not nv:
    #         return
    #     pipe = self.redis.pipeline()
    #     for (n, v), f in zip(nv.items(), fields):
    #         pipe.hset(f'{prefix}:{n}', f, v)
    #     await pipe.execute()

    # async def prefix_mhget(self, prefix: str, fields: list[str], keys: list[str] | None = None) -> dict[str, bytes]:
    #     if keys is None:
    #         keys = await self.redis.keys(prefix + b':*')
    #     pipe = self.redis.pipeline()
    #     for f, k in zip(fields, keys):
    #         pipe.hget(k, f)
    #     values = await pipe.execute()
    #     print('************')
    #     print(keys)
    #     print(values)
    #     # keys = [k.decode() for k in keys]
    #     keys = self.remove_table_prefix(keys)
    #     return dict(zip(keys, values))

    #     for key in keys:
    #         pipe.hmget(key, fields)
    #     values = await pipe.execute()
    #     keys = [k.decode() for k in keys]
    #     keys = self.remove_table_prefix(keys)
    #     return dict(zip(keys, values))

    # async def get_task_statuses(self, task_ids: list[str] | None = None) -> dict[str, bytes]:
    #     if task_ids is None:
    #         keys = await self.redis.keys(f'{self.prefix_tasks}:*')
    #     pipe = self.redis.pipeline()
    #     for key in keys:
    #         pipe.hget(key, 'status')
    #     statuses = await pipe.execute()
    #     keys = [k.decode() for k in keys]
    #     task_ids = self.remove_table_prefix(keys)
    #     return dict(zip(task_ids, statuses))
    #     # values = await self.redis.mget(keys)
    #     # return dict(zip(keys, values))

    # async def set_task_status(self, task_id: str, status: str):
    #     await self.redis.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)

    # async def get_task_status(self, task_id: str) -> str:
    #     return await self.redis.hget(f'{self.prefix_tasks}:{task_id}', 'status')

    # async def mset_task_status(self, statuses: dict[str, str]):
    #     if not statuses:
    #         return
    #     pipe = self.redis.pipeline()
    #     for task_id, status in statuses.items():
    #         pipe.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
    #     await pipe.execute()
    #     # statuses = {f'{self.prefix_tasks}:{k}': v for k, v in statuses.items()}
    #     # await self.redis.mset(statuses)
