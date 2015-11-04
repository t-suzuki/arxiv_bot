#!env python
# arXiv API wrapper with limited functionalities.
# reference: http://arxiv.org/help/api/index
import os
import sys
import time
import datetime
import urllib
import urllib2
import xml.etree.ElementTree

import dateutil.parser

def _throttle(delay_s):
    u'''throttled function call'''
    def _wrapper(f):
        def _f(*va, **kwa):
            funcs = getattr(_throttle, 'funcs', {})
            if funcs.has_key(f.__name__):
                s = funcs[f.__name__] - time.time()
                print('throttling.. delay: {:.2}'.format(s))
                time.sleep(s)
            res = f(*va, **kwa)
            funcs[f.__name__] = time.time() + delay_s
            setattr(_throttle, 'funcs', funcs)
            return res
        return _f
    return _wrapper

class ArXiv(object):
    throttle_s = 2.0
    def __init__(self, base_url='http://export.arxiv.org/api/query'):
        self.base_url = base_url

    @_throttle(throttle_s)
    def query(self, options):
        u'''access to arXiv and get the Atom response'''
        url = '{}?{}'.format(self.base_url, '&'.join('{}={}'.format(k, v) for k, v in options.items()))
        print url
        res = urllib2.urlopen(url)
        body = res.read()

        ns = {'ns': 'http://www.w3.org/2005/Atom'}
        root = xml.etree.ElementTree.fromstring(body)
        return root, ns

    def search_query(self, query_str, **kwa):
        u'''search_query API'''
        query_dict = dict(search_query=query_str)
        query_dict.update(kwa)
        root, ns = self.query(query_dict)
        res = []
        for n in root.findall('ns:entry', ns):
            try:
                url = n.find('ns:id', ns).text
            except AttributeError:
                url = 'N/A'
            try:
                title = n.find('ns:title', ns).text
            except AttributeError:
                title = 'N/A'
            try:
                summary = n.find('ns:summary', ns).text
            except AttributeError:
                summary = 'N/A'
            try:
                updated_at = dateutil.parser.parse(n.find('ns:updated', ns).text)
            except (AttributeError, ValueError, OverflowError):
                print( 'failed to get updated_at')
                updated_at = datetime.datetime.now()
            try:
                authors = [e.find('ns:name', ns).text for e in n.findall('./ns:author', ns)]
            except AttributeError:
                authors = []
            res.append(dict(
                url=url,
                title=title,
                summary=summary,
                authors=authors,
                updated_at=updated_at,
                ))
        return res

if __name__=='__main__':
    arxiv = ArXiv()
    print arxiv.search_query('au:Greg+AND+au:Egan')

