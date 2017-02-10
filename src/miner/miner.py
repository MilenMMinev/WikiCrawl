import logging
import time
import sys

import requests

from os import listdir
from os import path

import csv

import re
from threading import Thread
from random import shuffle

from links_queue import LinkQueue

THREADS_CNT = 50

# Downloaded pages counter
WIKI_DOMAIN = 'http://wikipedia.org'
PAGE_EARTH = 'https://en.wikipedia.org/wiki/Earth'

# A path where to store downloaded pages
WIKI_HTML_OUT_DIR = '../../data/raw/'
LINKS_CACHE = '.links.cache'

# Logger setup
logging.basicConfig(filename='../log.log', level=logging.INFO,
                    format='%(relativeCreated)6d %(threadName)s %(message)s')

# A queue of page links
l_queue = LinkQueue(maxsize=1000000)
l_queue.put(PAGE_EARTH)

curr_page = 0

def mine(max_articles, outdir=WIKI_HTML_OUT_DIR, crawl_width=100):
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
    while curr_page < max_articles:
        page_url = l_queue.pop()
        print(page_url)
        page_raw = fetch_article(page_url)

        logging.info('mine: saving article:{} of\t {}'.format(
            curr_page, max_articles))

        save_article(page_raw, outdir)
        page_ext_links = get_links(page_raw)
        l_queue.put_all(page_ext_links[:crawl_width])


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
    success = False

    source = ''
    while not success:
        try:
            source = requests.get(url).text
            success = True
        except:
            logging.error('unable to fetch {}. Will retry in 2sec.'.format(url))
            time.sleep(2)

    return source


def get_links(html):
    regex = r'a href="(/wiki/[\w]*?)"'
    links = re.findall(regex, html)
    links = list(map(lambda x: WIKI_DOMAIN + x, links))
    shuffle(links)
    logging.info('get_links: found {} links.'.format(len(links)))
    return links


def save_article(text, out_dir):
    """
    Manages files creation for pages
    The name of the file is a unique id
    """
    global curr_page
    assert(path.isdir(out_dir))

    f_name = path.join(out_dir, str(curr_page) + '.html')

    f = open(f_name, 'w')
    text = text.encode('utf-8')
    f.write(text)
    f.close()

    curr_page += 1

def main():
    assert len(sys.argv[1:]) == 1, "You must specify how many articles to mine"
    n_pages = int (sys.argv[1])

    bgn = time.time()

    start = time
    threads = [Thread(target=mine, args=((n_pages, )))
               for i in range(THREADS_CNT)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    total_time = time.time() - bgn
    print('mined {} pages in total {}. avg: {}'.format(
        curr_page, total_time, total_time / curr_page))

if __name__ == '__main__':
    main()
