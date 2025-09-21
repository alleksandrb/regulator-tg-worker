import os
import json
import asyncio
from urllib.parse import urlparse
from redis.exceptions import TimeoutError
from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesViewsRequest
from my_redis_init import init_redis
from TaskData import TaskData
from logger_configure import logger as log


QUEUE_NAME = os.getenv("QUEUE_NAME")
MAX_WORKERS = int(os.getenv("MAX_ASYNC_WORKERS"))
MAX_CONCURRENT_CONNECTIONS = int(os.getenv("MAX_CONCURRENT_CONNECTIONS"))
ACCOUNT_JSON_PATH = '/app/storage/json'
ACCOUNT_SESSION_PATH = '/app/storage/sessions'

telegram_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)
account_locks = {}


async def process_task(task_data: TaskData, account_lock: asyncio.Lock, worker_name: str):
    """
    Обработка одной задачи: подключение к Telegram и инкремент просмотра поста.
    """
    try:
        client = TelegramClient(
            session=task_data.get_session_file_path(),
            api_id=task_data.get_api_id(),
            api_hash=task_data.get_api_hash(),
            proxy=task_data.get_proxy_config(),
            connection_retries=2,
            retry_delay=1,
            timeout=30,
        )

        # Use semaphore to limit concurrent connections and prevent race conditions
        async with telegram_semaphore:
            # Разбор ссылки
            parts = urlparse(task_data.get_telegram_post_url()).path.strip("/").split("/")
            channel_username, message_id = parts[0], int(parts[1])

            async with account_lock:
                await client.connect()
                await client.get_me()
                entity = await client.get_entity(channel_username)
                
                await client(
                    GetMessagesViewsRequest(
                        peer=entity,
                        id=[message_id],
                        increment=True,
                    )
                )
                log.info(
                    f"[Worker-{worker_name}] Просмотр добавлен {task_data.get_telegram_post_url()}",
                )
                await client.disconnect()

    except Exception as e:
        log.error(f"[Worker-{worker_name}] Ошибка при обработке задачи: {e}")

# ==============================
# Worker loop
# ==============================
async def worker(name: int, redis):
    log.info(f"[Worker-{name}] запущен")

    while True:
        try:
            result = await redis.blpop(QUEUE_NAME, timeout=10)
            if result:
                _, task_json = result
                task_data = TaskData(json.loads(task_json), ACCOUNT_JSON_PATH, ACCOUNT_SESSION_PATH)

                log.info(f"[Worker-{name}] Получена задача: {task_data.get_telegram_post_url()}")
                account_id = task_data.get_account_id()

                if account_id not in account_locks:
                    account_locks[account_id] = asyncio.Lock()

                await process_task(task_data, account_locks[account_id], name)
                await asyncio.sleep(1)

        except TimeoutError:
            # Это просто истек таймаут ожидания задачи → не ошибка
            log.info(f"[Worker-{name}] Нет задач (таймаут)")

        except Exception as e:
            log.error(
                f"[Worker-{name}] Ошибка: {e}"
            )
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
