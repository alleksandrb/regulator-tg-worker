import os
import json
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
from redis.exceptions import TimeoutError
from my_redis_init import init_redis_sync
from logger_configure import logger as log
import subprocess

QUEUE_NAME = os.getenv("QUEUE_NAME")
MAX_PARALLEL_PROCESSES = int(os.getenv("MAX_PARALLEL_PROCESSES", 50))

def run_task(task_json_str: str):
    """Функция запуска отдельного Python процесса для одной задачи."""
    try:
        subprocess.run(
            ["python", "one_task.py", task_json_str],
            check=True,
            timeout=60
        )
    except subprocess.CalledProcessError as e:
        log.error(f"[Worker-Process] Ошибка выполнения задачи: {e}")
    except subprocess.TimeoutExpired:
        log.error(f"[Worker-Process] Таймаут выполнения задачи")
    except Exception as e:
        log.error(f"[Worker-Process] Непредвиденная ошибка: {e}")

def main():
    log.info("[Worker-Manager] Запущен")
    redis = init_redis_sync()

    # Создаём пул процессов
    with ProcessPoolExecutor(max_workers=MAX_PARALLEL_PROCESSES) as executor:
        futures = set()

        while True:
            try:
                result = redis.blpop(QUEUE_NAME, timeout=10)
                if not result:
                    log.info("[Worker-Manager] Нет задач (таймаут)")
                    continue

                _, task_json_str = result
                task_dict = json.loads(task_json_str)

                # Добавляем задачу в пул процессов
                future = executor.submit(run_task, task_json_str)
                futures.add(future)

                log.info(f"[Worker-Manager] Запущена задача: Account ID: {task_dict['account_id']} "
                         f"Telegram Post URL: {task_dict['telegram_post_url']}")

                # Удаляем завершённые процессы из множества futures
                done = {f for f in futures if f.done()}
                futures -= done

            except TimeoutError:
                log.info("[Worker-Manager] Нет задач (таймаут)")

            except Exception as e:
                log.error(f"[Worker-Manager] Ошибка: {e}")

if __name__ == "__main__":
    main()
