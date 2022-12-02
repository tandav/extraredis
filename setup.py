import os
from pathlib import Path

import unasync
from setuptools import setup

ADDITIONAL_REPLACEMENTS = {
    'redis_asyncio': 'redis_sync',
    'fake_redis_async': 'fake_redis_sync',
    'pytest_mark_asyncio': 'pytest_mark_sync',
    'pytest_asyncio': 'pytest',
    'ExtraRedisAsync': 'ExtraRedis',
}


def make_sync():
    rules = [
        unasync.Rule(
            fromdir='/extraredis/_async/',
            todir='/extraredis/_sync/',
            additional_replacements=ADDITIONAL_REPLACEMENTS,
        ),
        unasync.Rule(
            fromdir='/tests/_async/',
            todir='/tests/_sync/',
            additional_replacements=ADDITIONAL_REPLACEMENTS,
        ),
    ]
    filepaths = []
    for root, _, filenames in os.walk(Path(__file__).absolute().parent):
        for filename in filenames:
            if filename.rpartition('.')[-1] in (
                'py',
                'pyi',
            ):
                filepaths.append(os.path.join(root, filename))

    unasync.unasync_files(filepaths, rules)


make_sync()
setup()
