import twitter

from util import COUNTRIES, ICOUNTRY_ECOUNTRY_MAP


class TwitterHandler:

    def __init__(self, token: str, token_secret: str, consumer_key: str, consumer_secret: str) -> None:
        self.token = token
        self.token_secret = token_secret
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret

    def post(self, text: str) -> None:
        auth = twitter.OAuth(self.token, self.token_secret, self.consumer_key, self.consumer_secret)
        t = twitter.Twitter(auth=auth)
        t.statuses.update(status=text)

    @staticmethod
    def create_text(d: dict) -> str:
        title = d['ja_translated']['title']
        country = ''
        ecountry = ICOUNTRY_ECOUNTRY_MAP[d['displayed_country']]
        for country_dict in COUNTRIES:
            if ecountry == country_dict['country']:
                country = country_dict['name']['ja']
        assert country != ''
        domain = d['ja_domain_label']
        if d['topics']:
            sorted_topics = sorted(d['topics'].items(), key=lambda x: -x[1])
            topic = sorted_topics[0][0]
            content = f'{title}（{country}，{topic}のニュース，{domain}）'
        else:
            content = f'{title}（{country}のニュース，{domain}）'
        return content + '\n' + 'https://lotus.kuee.kyoto-u.ac.jp/NLPforCOVID-19'
