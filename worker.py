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
from telethon.errors import RPCError, FloodWaitError


QUEUE_NAME = "view-increment-queue"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MAX_WORKERS = int(os.getenv("MAX_ASYNC_WORKERS", 10))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 3))
# Limit concurrent Telegram connections to prevent race conditions
MAX_CONCURRENT_CONNECTIONS = int(os.getenv("MAX_CONCURRENT_CONNECTIONS", 3))

# Global semaphore to limit concurrent Telegram connections
telegram_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)

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
            "extra": getattr(record, "extra", {}),
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
    Включает автоматический retry при сетевых ошибках.
    """
    temp_file = None
    try:
        account_json = task_data["account_json_data"]
        user_id = account_json["user_id"]

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
        with tempfile.NamedTemporaryFile(suffix=".session", delete=False) as temp_file_obj:
            temp_file_obj.write(session_bytes)
            temp_file_obj.flush()
            temp_file = temp_file_obj.name

        client = TelegramClient(
            temp_file,
            account_json["app_id"],
            account_json["app_hash"],
            proxy=proxy_config,
            connection_retries=2,
            retry_delay=1,
            timeout=30,
        )

        # Use semaphore to limit concurrent connections and prevent race conditions
        async with telegram_semaphore:
            # Small delay to reduce connection race conditions
            await asyncio.sleep(0.1)
            
            telegram_post_url = task_data["telegram_post_url"]
            # Разбор ссылки
            parts = urlparse(telegram_post_url).path.strip("/").split("/")
            channel_username, message_id = parts[0], int(parts[1])

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # Переподключаемся на каждой попытке для надежности
                    async with client:
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
                        break  # если успешно — выходим из retry
                        
                except (ConnectionError, RPCError, FloodWaitError, RuntimeError, OSError, BrokenPipeError, ConnectionResetError) as e:
                    error_msg = str(e)
                    if attempt < MAX_RETRIES:
                        wait_time = RETRY_DELAY * attempt
                        logger.warning(
                            f"[Task] Сетевая ошибка: {error_msg}, повтор через {wait_time}s (попытка {attempt}/{MAX_RETRIES})", 
                            extra={"user_id": user_id, "error_type": type(e).__name__, "url": telegram_post_url}
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(
                            f"[Task] Не удалось выполнить после {MAX_RETRIES} попыток: {error_msg}", 
                            extra={"user_id": user_id, "error_type": type(e).__name__, "url": telegram_post_url}, 
                            exc_info=True
                        )

    except Exception as e:
        logger.error(f"[Task] Ошибка при обработке задачи: {e}", extra={"user_id": user_id}, exc_info=True)

    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception:
                pass

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
                    extra={"telegram_post_url": task_data["telegram_post_url"]},
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
    workers = [worker(i, redis) for i in range(MAX_WORKERS)]
    await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())
