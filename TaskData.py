import json

class TaskData:
    def __init__(self, task_data: dict, account_json_path: str, account_session_path: str):
        self.__account_json_path = account_json_path
        self.__account_session_path = account_session_path
        self.__account_id = task_data["account_id"]
        self.__account = json.load(open(f"{self.__account_json_path}/{self.__account_id}.json"))
        self.__telegram_post_url = task_data["telegram_post_url"]
        self.__proxy = task_data["proxy"]
    
    def get_proxy_config(self) -> dict:
        return {
            "proxy_type": self.__proxy.get("protocol"),
            "addr": self.__proxy.get("ip"),
            "port": int(self.__proxy.get("port")),
            "username": self.__proxy.get("login"),
            "password": self.__proxy.get("password"),
        }
    
    def get_api_id(self) -> int:
        return self.__account.get("app_id")
    
    def get_api_hash(self) -> str:
        return self.__account.get("app_hash")

    def get_account_id(self) -> int:
        return self.__account_id

    def get_session_file_path(self) -> str:
        return f"{self.__account_session_path}/{self.__account_id}.session"
    
    def get_telegram_post_url(self) -> str:
        return self.__telegram_post_url