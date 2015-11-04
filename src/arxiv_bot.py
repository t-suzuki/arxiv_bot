#!env python
# tweet new papers in arXiv cs.CV
import os
import sys
import sqlite3
import time
import datetime
import random
import argparse
import traceback
import logging
import logging.handlers

import arxiv_api
import twitter_api

def setup_log(log_file):
    formatter = logging.Formatter('%(asctime)-15s [%(levelname)-8s]: %(message)s')
    logger = logging.getLogger('arxiv_bot')
    logger.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(filename=log_file, maxBytes=10*1024*1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def register_logger():
    logger = logging.getLogger('arxiv_bot')
    globals()['logger'] = logger

class Entries(object):
    def __init__(self, db_path='tweeted_entries.db'):
        self.path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_table()

    def delete(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        os.unlink(self.path)

    def schema(self):
        return ['url text primary key', 'title text', 'authors text', 'summary text', 'updated_at date', 'tweeted_at date']

    def names(self):
        return [column.split(' ')[0] for column in self.schema()]

    def create_table(self):
        with self.conn:
            self.conn.execute('CREATE TABLE IF NOT EXISTS entries ({})'.format(', '.join(self.schema())))

    def has_entry(self, url):
        with self.conn:
            for row in self.conn.execute('SELECT count(url) FROM entries WHERE url=?', (url,)):
                count = row[0]
                return count > 0

    def add_or_update_entry(self, entry):
        entry_tuple = (
                entry['url'],
                entry['title'],
                ', '.join(entry['authors']),
                entry['summary'],
                entry['updated_at'],
                entry.get('tweeted_at', ''),
                )
        if self.has_entry(entry['url']):
            with self.conn:
                self.conn.execute('UPDATE entries SET title=?, authors=?, summary=?, updated_at=?, tweeted_at=? WHERE url=?',
                        entry_tuple[1:] + entry_tuple[:1])
            return False
        else:
            with self.conn:
                self.conn.execute('INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?)', entry_tuple)
            return True

    def get_untweeted_entries(self):
        entries = []
        with self.conn:
            for row in self.conn.execute('SELECT url, title, authors, summary, updated_at, tweeted_at FROM entries WHERE tweeted_at = ""'):
                entry = self.parse_entry(row)
                entries.append(entry)
        return entries

    def parse_entry(self, row):
        e = dict(zip(self.names(), row))
        if ', ' in e['authors']:
            e['authors'] = e['authors'].split(', ')
        else:
            e['authors'] = [e['authors']]
        return e

    def list_entry(self):
        with self.conn:
            for row in self.conn.execute('SELECT * FROM entries'):
                print row


class ArXivBot(object):
    def __init__(self, category, entries_db, arxiv_api_obj, twitter_api_obj, max_tweet=10):
        self.category = category
        self.db = entries_db
        self.arxiv = arxiv_api_obj
        self.twitter = twitter_api_obj
        self.max_tweet = max_tweet
        self.tweet_throttle_s = 2.0

    def fetch_new_papers(self):
        max_results = 100
        count = 0
        for entry in self.arxiv.search_query('cat:{}'.format(self.category), max_results=100, sortBy='lastUpdatedDate', sortOrder='descending'):
            was_new = self.db.add_or_update_entry(entry)
            count += 1
            msg = 'added: {} ({}) (new? {})'.format(entry['title'], entry['updated_at'], was_new)
            if was_new:
                logger.info(msg)
            else:
                logger.debug(msg)
        return count

    def tweet_untweeted(self):
        count = 0
        entries = list(self.db.get_untweeted_entries())
        random.shuffle(entries)
        for entry in entries[:self.max_tweet]:
            text = self.format_entry(entry)
            succeeded = self.twitter.tweet(text)
            if succeeded:
                entry['tweeted_at'] = datetime.datetime.now()
                self.db.add_or_update_entry(entry)
                count += 1
                logger.info('tweeted: {title} (len {len}, {updated_at})'.format(len=len(text), **entry))
            else:
                logger.error('failed to tweet: {title} (len {len}, {updated_at})'.format(len=len(text), **entry))
            time.sleep(self.tweet_throttle_s)
        return count

    def format_entry(self, entry):
        max_len = 140
        fmt = '{title}. {url}'
        exceed = len(fmt.format(**entry)) - max_len
        if exceed > 0:
            res = fmt.format(title=entry['title'][:-exceed-2]+'..', url=entry['url'])
        else:
            res = fmt.format(**entry)
        return res

def arxiv_bot_job(category, twitter, interval_s, max_tweet):
    logger.info('Starting arXiv bot on [{}]..'.format(category))
    db = Entries()
    arxiv = arxiv_api.ArXiv()
    bot = ArXivBot(category, db, arxiv, twitter, max_tweet)
    while True:
        logger.info('-'*80)
        n_fetched = bot.fetch_new_papers()
        logger.info('total fetched: {}'.format(n_fetched))
        logger.info('-'*80)
        n_tweeted = bot.tweet_untweeted()
        logger.info('total tweeted: {}'.format(n_tweeted))
        logger.info('sleep {}'.format(interval_s))
        time.sleep(interval_s)

def _test_delete_db():
    db = Entries()
    db.delete()

def _test_add_greg_egan_papers():
    arxiv = arxiv_api.ArXiv()
    papers = arxiv.search_query('au:Greg+AND+au:Egan')
    db = Entries()
    for p in papers:
        db.add_or_update_entry(p)
    db.list_entry()

def _test_update_tweeted_at():
    arxiv = arxiv_api.ArXiv()
    papers = arxiv.search_query('au:Greg+AND+au:Egan')
    db = Entries()
    p = papers[0]
    p['tweeted_at'] = datetime.datetime.now()
    db.add_or_update_entry(p)
    db.list_entry()

def _test_get_untweeted_entries_and_tweet():
    db = Entries()
    for e in db.get_untweeted_entries():
        print 'tweet', e
        e['tweeted_at'] = datetime.datetime.now()
        db.add_or_update_entry(e)
    db.list_entry()

def _test_cs_CV():
    arxiv = arxiv_api.ArXiv()
    for p in arxiv.search_query('cat:cs.CV', max_results=5):
        print p

def _test_format():
    db = Entries()
    arxiv = arxiv_api.ArXiv()
    tw = twitter_api.Twitter.from_file('twitter.ini', 'test_account')
    bot = ArXivBot('cs.CV', db, arxiv, tw)
    s = bot.format_entry({'title': 'a'*140, 'url':'b'*20})
    print len(s), s

def _test_bot(use_dummy=True):
    db = Entries()
    arxiv = arxiv_api.ArXiv()
    if use_dummy:
        tw = twitter_api.DummyTwitter()
    else:
        tw = twitter_api.Twitter.from_file('twitter.ini', 'test_account')
    bot = ArXivBot('cs.CV', db, arxiv, tw)
    print '-'*80
    n_fetched = bot.fetch_new_papers()
    print 'total fetched:', n_fetched
    print '-'*80
    n_tweeted = bot.tweet_untweeted()
    print 'total tweeted:', n_tweeted

def main():
    parser = argparse.ArgumentParser('arXiv bot')
    parser.add_argument('--twitter', default=None, type=str,
            help='<twitter ini file>@<section> (dummy if not specified)')
    parser.add_argument('--interval', default=6*60*60, type=float,
            help='sleep after iteration (s)')
    parser.add_argument('--rebooting', action='store_true', default=False,
            help='recover the bot from error and continue')
    parser.add_argument('--max-tweet', default=10, type=int,
            help='limit successive tweets')
    parser.add_argument('--category', type=str, required=True,
            help='arXiv category to follow')
    parser.add_argument('--log', default='arxiv_bot.log', type=str,
            help='log file')

    args = parser.parse_args()

    twitter = None
    if args.twitter is not None:
        file_path, section = args.twitter.split('@')
        twitter = twitter_api.Twitter.from_file(file_path, section)
    if twitter is None:
        twitter = twitter_api.DummyTwitter()

    # log
    setup_log(args.log)
    register_logger()
    logger.info(args)

    # job
    while True:
        try:
            arxiv_bot_job(args.category, twitter, args.interval, args.max_tweet)
        except:
            exc = traceback.format_exc()
            logger.error('Exception caught:' + exc)
            if args.rebooting:
                logger.warn('rebooting')
            else:
                logger.warn('terminating')
                break

if __name__=='__main__':
    #_test_delete_db()
    #_test_add_greg_egan_papers()
    #_test_update_tweeted_at()
    #_test_get_untweeted_entries_and_tweet()
    #_test_cs_CV()
    #_test_bot()
    main()



