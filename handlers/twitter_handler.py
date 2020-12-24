import twitter


class TwitterHandler:

    def __init__(self, token: str, token_secret: str, consumer_key: str, consumer_secret: str) -> None:
        self.token = token
        self.token_secret = token_secret
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret

    def post(self, text: str) -> None:
        auth = twitter.OAuth(self.token, self.token_secret, self.consumer_key, self.consumer_secret)
        t = twitter.Twitter(auth=auth)
        t.statuses.update(text)

    @staticmethod
    def create_text(d: dict) -> str:
        if d['topic']:
            content = f'{d["title"]}（{d["country"]}，{d["topic"]}のニュース，{d["domain"]}）'
        else:
            content = f'{d["title"]}（{d["country"]}のニュース，{d["domain"]}）'
        return content + '\n' + 'https://lotus.kuee.kyoto-u.ac.jp/NLPforCOVID-19'
