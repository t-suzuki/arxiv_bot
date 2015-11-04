#!env python
# simple Twitter client using requests_oauthlib.
import requests_oauthlib
import ConfigParser

class Twitter(object):
    def __init__(self, consumer_key, consumer_secret, access_token, access_secret):
        self.api = requests_oauthlib.OAuth1Session(
                consumer_key, consumer_secret,
                access_token, access_secret)

    def tweet(self, message):
        url = 'https://api.twitter.com/1.1/statuses/update.json'
        req = self.api.post(url, params={'status': message})
        if req.status_code == 200:
            return True
        return False

    @classmethod
    def from_file(cls, ini_file, section):
        with open(ini_file, 'rt') as fi:
            conf = ConfigParser.ConfigParser()
            conf.readfp(fi)
        if conf.has_section(section):
            items = dict(conf.items(section))
            try:
                consumer_key    = items['consumer_key']
                consumer_secret = items['consumer_secret']
                access_token    = items['access_token']
                access_secret   = items['access_secret']
            except KeyError:
                return None
            return Twitter(consumer_key, consumer_secret, access_token, access_secret)
        return None

if __name__=='__main__':
    tw = Twitter.from_file('twitter.ini', 'test_account')
    if tw is not None:
        tw.tweet('dev test')

