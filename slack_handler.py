import requests


class SlackHandler:
    def __init__(self, access_token: str, app_channel: str) -> None:
        self.access_token = access_token
        self.app_channel = app_channel

    def post(self, text: str) -> None:
        requests.post(
            "https://slack.com/api/chat.postMessage",
            data={
                "token": self.access_token,
                "channel": self.app_channel,
                "text": text,
            },
        )
