import os
from pathlib import Path

import unasync

ADDITIONAL_REPLACEMENTS = {
    'aextraredis': 'extraredis',
    'redis_asyncio': 'redis_sync',
    'fake_redis_async': 'fake_redis_sync',
    'pytest_mark_asyncio': 'pytest_mark_sync',
    'pytest_asyncio': 'pytest',
}


def main():
    rules = [
        unasync.Rule(
            fromdir='/aextraredis/',
            todir='/extraredis/',
            additional_replacements=ADDITIONAL_REPLACEMENTS,
        ),
        unasync.Rule(
            fromdir='/tests/async/',
            todir='/tests/sync/',
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


if __name__ == '__main__':
    main()
