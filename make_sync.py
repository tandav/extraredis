
# import unasync

# ADDITIONAL_REPLACEMENTS = {
#     'redis_asyncio': 'redis_sync',
#     'fake_redis_async': 'fake_redis_sync',
#     'pytest_mark_asyncio': 'pytest_mark_sync',
#     'pytest_asyncio': 'pytest',
#     'ExtraRedisAsync': 'ExtraRedis',
# }


# def main():
#     rules = [
#         unasync.Rule(
#             fromdir='/extraredis/_async/',
#             todir='/extraredis/_sync/',
#             additional_replacements=ADDITIONAL_REPLACEMENTS,
#         ),
#         unasync.Rule(
#             fromdir='/tests/_async/',
#             todir='/tests/_sync/',
#             additional_replacements=ADDITIONAL_REPLACEMENTS,
#         ),
#     ]


# if __name__ == '__main__':
#     main()
