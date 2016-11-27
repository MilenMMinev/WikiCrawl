import logging
import time

import urllib.request
import urllib.response

from os import listdir
from os import path

import csv

import re
from threading import Thread
from random import shuffle

from links_queue import LinkQueue

THREADS_CNT = 100

# Downloaded pages counter
PAGE_CNT = 0
WIKI_DOMAIN = 'http://wikipedia.org'
PAGE_EARTH = 'https://en.wikipedia.org/wiki/Earth'

# A path where to store downloaded pages
WIKI_HTML = '../../data/raw/'
LINKS_CACHE = '.links.cache'

# Logger setup
logging.basicConfig(filename='..\log.log', level=logging.INFO,
                    format='%(relativeCreated)6d %(threadName)s %(message)s')

# A queue of page links
l_queue = LinkQueue(maxsize=1000000)
l_queue.put(PAGE_EARTH)

"""
Provide a function to crawl wikipedia pages.
"""


def mine(max_articles, outdir=WIKI_HTML, crawl_width=100):
    """
    @brief Start Crawling the elements in the list queue
           Pages are saved as html in outdir
    @param max_articles: Number of pages that will be downloaded
                         Note that if a thread has already started mining
                         it will not be terminated and slightly bigger Number
                         of pages is expected
    @crawl_width         Max count of links that will be extracted from a page
    """
    global l_queue
    while PAGE_CNT < max_articles:
        page_url = l_queue.pop()
        page_raw = fetch_article(page_url)
        logging.info('mine: saving article:{} of\t {}'.format(
            PAGE_CNT, max_articles))
        save_article(page_raw)
        page_ext_links = get_links(page_raw)
        l_queue.put_all(page_ext_links[:100])


def dump_links(links):
    """
    @brief dump crawled links to a file to be later reused
    """
    with open(LINKS_CACHE, 'w') as cache:
        wr = csv.writer(cache, quoting=csv.QUOTE_ALL)
        wr.writerow(links)
        cache.close()


def fetch_article(url):
    """
    Return raw html from url
    """
    source = urllib.request.urlopen(url, timeout=10).read()
    source = source.decode('utf-8')
    return source


def get_links(html):
    regex = r'a href="(/wiki/[\w]*?)"'
    links = re.findall(regex, html)
    links = list(map(lambda x: WIKI_DOMAIN + x, links))
    shuffle(links)
    logging.info('get_links: found {} links.'.format(len(links)))
    return links


def save_article(text, out_dir=WIKI_HTML):
    """
    Manages files creation for pages
    The name of the file is a unique id
    """
    global PAGE_CNT
    assert(path.isdir(out_dir))

    f_name = path.join(out_dir, str(PAGE_CNT) + '.html')
    PAGE_CNT += 1

    f = open(f_name, 'w')
    f.write(text)


def main():
    bgn = time.time()

    start = time
    threads = [Thread(target=mine, args=((30000, )), daemon=True)
               for i in range(THREADS_CNT)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    total_time = time.time() - bgn
    print('mined {} pages in total {}. avg: {}'.format(
        PAGE_CNT, total_time, total_time / PAGE_CNT))

if __name__ == '__main__':
    main()
