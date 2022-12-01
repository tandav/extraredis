__version__ = '0.0.1'
import os
import dotenv

from redis.asyncio import Redis


class ExtraRedis:
    def __init__(self, redis: Redis | None = None):
        dotenv.load_dotenv()
        self.redis = redis or Redis(
            host=os.environ['REDIS_HOST'],
            port=os.environ['REDIS_PORT'],
            password=os.environ['REDIS_PASSWORD'],
        )
        self.prefix_tasks = 'tasks'

    # @staticmethod
    # def remove_key_level(keys: list[str], level: int) -> list[str]:
    #     return [key.split(':')[level] for key in keys]
    #     return [key.split(':')[1] for key in keys]

    @staticmethod
    def remove_table_prefix(keys: list[bytes]) -> list[bytes]:
        return [key.split(b':')[1] for key in keys]

    async def prefix_mget(self, prefix: bytes, keys: list[str] | None = None) -> dict[str, bytes]:
        if keys is None:
            keys = await self.redis.keys(prefix + b':*')
        values = await self.redis.mget(keys)
        keys = self.remove_table_prefix(keys)
        return dict(zip(keys, values))

    async def prefix_mhgetall(self, prefix: str, keys: list[str] | None = None) -> dict[str, bytes]:
        keys = await self.redis.keys(f'{prefix}:*')
        pipe = self.redis.pipeline()
        for key in keys:
            pipe.hgetall(key)
        values = await pipe.execute()
        keys = [k.decode() for k in keys]
        return dict(zip(keys, values))

    async def prefix_mhset(self, prefix: str, nv: dict[str, str], fields: list[str] | None = None):
        if not nv:
            return
        pipe = self.redis.pipeline()
        for (n, v), f in zip(nv.items(), fields):
            pipe.hset(f'{prefix}:{n}', f, v)
        await pipe.execute()

    async def prefix_mhget(self, prefix: str, fields: list[str], keys: list[str] | None = None) -> dict[str, bytes]:
        if keys is None:
            keys = await self.redis.keys(prefix + b':*')
        pipe = self.redis.pipeline()
        for f, k in zip(fields, keys):
            pipe.hget(k, f)
        values = await pipe.execute()
        print('************')
        print(keys)
        print(values)
        # keys = [k.decode() for k in keys]
        keys = self.remove_table_prefix(keys)
        return dict(zip(keys, values))

        for key in keys:
            pipe.hmget(key, fields)
        values = await pipe.execute()
        keys = [k.decode() for k in keys]
        keys = self.remove_table_prefix(keys)
        return dict(zip(keys, values))

    async def get_task_statuses(self, task_ids: list[str] | None = None) -> dict[str, bytes]:
        if task_ids is None:
            keys = await self.redis.keys(f'{self.prefix_tasks}:*')
        pipe = self.redis.pipeline()
        for key in keys:
            pipe.hget(key, 'status')
        statuses = await pipe.execute()
        keys = [k.decode() for k in keys]
        task_ids = self.remove_table_prefix(keys)
        return dict(zip(task_ids, statuses))
        # values = await self.redis.mget(keys)
        # return dict(zip(keys, values))

    async def set_task_status(self, task_id: str, status: str):
        await self.redis.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
    
    async def get_task_status(self, task_id: str) -> str:
        return await self.redis.hget(f'{self.prefix_tasks}:{task_id}', 'status')

    async def mset_task_status(self, statuses: dict[str, str]):
        if not statuses:
            return
        pipe = self.redis.pipeline()
        for task_id, status in statuses.items():
            pipe.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
        await pipe.execute()
        # statuses = {f'{self.prefix_tasks}:{k}': v for k, v in statuses.items()}
        # await self.redis.mset(statuses)
