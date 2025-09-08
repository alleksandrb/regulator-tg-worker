import os
import json
import base64
import tempfile
import asyncio
import logging
import sys
from urllib.parse import urlparse
from redis.exceptions import TimeoutError
import redis.asyncio as aioredis
from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesViewsRequest


QUEUE_NAME = "view-increment-queue"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MAX_WORKERS = int(os.getenv("MAX_ASYNC_WORKERS", 10))


# ==============================
# JSON логирование
# ==============================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)


# ==============================
# Redis init
# ==============================
async def init_redis():
    redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=5,
    )
    await redis.ping()
    logger.info("[Redis] Подключение успешно")
    return redis


# ==============================
# Task processor
# ==============================
async def process_task(task_data: dict):
    """
    Обработка одной задачи: подключение к Telegram и инкремент просмотра поста.
    """
    try:
        account_json = task_data["account_json_data"]

        # Прокси
        proxy_config = None
        if task_data.get("proxy"):
            proxy = task_data["proxy"]
            proxy_config = {
                "proxy_type": proxy.get("protocol"),
                "addr": proxy.get("ip"),
                "port": int(proxy.get("port")),
                "username": proxy.get("login"),
                "password": proxy.get("password"),
            }

        # Сессия
        session_bytes = base64.b64decode(task_data["session"])
        with tempfile.NamedTemporaryFile(suffix=".session") as temp_file:
            temp_file.write(session_bytes)
            temp_file.flush()

            client = TelegramClient(
                temp_file.name,
                account_json["app_id"],
                account_json["app_hash"],
                proxy=proxy_config,
            )

            async with client:
                telegram_post_url = task_data["telegram_post_url"]

                # Разбор ссылки
                parts = urlparse(telegram_post_url).path.strip("/").split("/")
                channel_username, message_id = parts[0], int(parts[1])

                entity = await client.get_entity(channel_username)
                result = await client(
                    GetMessagesViewsRequest(
                        peer=entity,
                        id=[message_id],
                        increment=True,
                    )
                )
                logger.info(
                    f"[Task] Просмотр добавлен",
                    extra={"url": telegram_post_url, "result": str(result)},
                )

    except Exception as e:
        logger.error(f"[Task] Ошибка: {e}", exc_info=True)


# ==============================
# Worker loop
# ==============================
async def worker(name: int, redis):
    logger.info(f"Воркер-{name} запущен")
    while True:
        try:
            result = await redis.blpop(QUEUE_NAME, timeout=10)
            if result:
                _, task_json = result
                task_data = json.loads(task_json)
                logger.info(
                    f"[Worker-{name}] Получена задача",
                    extra={"task": task_data},
                )
                await process_task(task_data)

        except TimeoutError:
            # Это просто истек таймаут ожидания задачи → не ошибка
            logger.debug(f"[Worker-{name}] Нет задач (таймаут)")

        except Exception as e:
            logger.error(f"[Worker-{name}] Ошибка: {e}", exc_info=True)
            await asyncio.sleep(1)


# ==============================
# Main
# ==============================
async def main():
    redis = await init_redis()
    workers = [worker(i, redis) for i in range(MAX_WORKERS)]  # 3 асинхронных воркера
    await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())
