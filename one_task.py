from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesViewsRequest
from urllib.parse import urlparse
import sys
import json
from TaskData import TaskData

ACCOUNT_JSON_PATH = '/app/storage/json'
ACCOUNT_SESSION_PATH = '/app/storage/sessions'

async def main(task_data: TaskData):
    parts = urlparse(task_data.get_telegram_post_url()).path.strip("/").split("/")
    channel_username, message_id = parts[0], int(parts[1])

    entity = await client.get_entity(channel_username)
    await client(
        GetMessagesViewsRequest(
            peer=entity,
            id=[message_id],
            increment=True,
        )
    )
    print(f"Account ID: {task_data.get_account_id()} Пост просмотрен {task_data.get_telegram_post_url()}")
    exit(0)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        task_json = sys.argv[1]
        task_data_json = json.loads(task_json)
        task_data = TaskData(task_data_json, ACCOUNT_JSON_PATH, ACCOUNT_SESSION_PATH)

        client = TelegramClient(
            task_data.get_session_file_path(),
            api_id=task_data.get_api_id(),
            api_hash=task_data.get_api_hash(),
            proxy=task_data.get_proxy_config(),
            receive_updates=False,
        )

        with client:
            client.loop.run_until_complete(main(task_data))
        
    else:
        print("Usage: python one.py <task_json>")