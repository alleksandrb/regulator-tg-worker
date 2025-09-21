import redis.asyncio as aioredis
import redis
import os
# ==============================
# Redis init
# ==============================
async def init_redis():
    redis = aioredis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        decode_responses=True,
        socket_timeout=5,
    )
    await redis.ping()
    print("[Redis] Подключение успешно")
    return redis

def init_redis_sync():
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        decode_responses=True,
        socket_timeout=5,
    )
    redis_client.ping()
    print("[Redis] Подключение успешно")
    return redis_client