#!env python
# tweet new papers in arXiv cs.CV
import os
import sys
import sqlite3
import datetime
import random

import arxiv_api
import twitter_api

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
    def __init__(self, category, entries_db, arxiv_api_obj, twitter_api_obj):
        self.category = category
        self.db = entries_db
        self.arxiv = arxiv_api_obj
        self.twitter = twitter_api_obj

    def fetch_new_papers(self):
        max_results = 100
        count = 0
        for entry in self.arxiv.search_query('cat:{}'.format(self.category), max_results=100, sortBy='lastUpdatedDate', sortOrder='descending'):
            was_new = self.db.add_or_update_entry(entry)
            count += 1
            print('added: {} ({}) (new? {})'.format(entry['title'], entry['updated_at'], was_new))
        return count

    def tweet_untweeted(self):
        max_tweet = 2
        count = 0
        entries = list(self.db.get_untweeted_entries())
        random.shuffle(entries)
        for entry in entries[:max_tweet]:
            succeeded = self.twitter.tweet(self.format_entry(entry))
            if succeeded:
                entry['tweeted_at'] = datetime.datetime.now()
                self.db.add_or_update_entry(entry)
                count += 1
                print('tweeted: {title} ({updated_at})'.format(**entry))
            else:
                print('failed to tweet: {title} ({updated_at})'.format(**entry))
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

if __name__=='__main__':
    #_test_delete_db()
    #_test_add_greg_egan_papers()
    #_test_update_tweeted_at()
    #_test_get_untweeted_entries_and_tweet()
    #_test_cs_CV()
    _test_bot()



