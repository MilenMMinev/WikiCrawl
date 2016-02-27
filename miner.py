import urllib.request
import urllib.response
import time
import re
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread, Lock
import logging

import sys
from os import listdir
from constants import *

# amount of links crawler visits per page
CRAWLER_WIDTH = 20
THREADS = 4

visited = set()

linkBuffer = set()
linkBuffer.add(ARTICLE_URL)

visitedLock = Lock()
bufferLock = Lock()

# logging.basicConfig(filename='miner.log', level=logging.INFO)
logging.basicConfig(level=logging.INFO)


class Miner(Thread):

    def __init__(self, index):
        Thread.__init__(self)
        self.index = index

    def run(self):
        logging.info('Starting thread {}'.format(self.getName()))
        while True:
            if len(linkBuffer) == 0:
                time.sleep(1)
                continue
            self.mine()

    def getArticleTitle(self, source):
        return re.findall(r"<title>(.*) - Wikipedia", source)[0]

    def getArticleLinks(self, source, limit=10**6):
        relativeLinks = []
        wikiDomain = "http://wikipedia.org"

        try:
            soup = BeautifulSoup(source, "lxml")
            links = list(soup.find_all('a', href=True))
            for link in links:
                link = str(link)
                if re.search(r'href=\"/wiki/\w+\"', link):
                    link = re.search(r'href=\"/wiki/\w+\"', link).group()
                    link = re.match(r'href=\"(.*)\"', link).group(0)
                    relativeLinks.append(wikiDomain + link[6:-1])
                    limit = limit - 1
                    if limit == 0:
                        continue
            return relativeLinks
        except:
            logging.error('exception in getArticleLinks')
            sys.exit()

# saves article text to file
    def mine(self, TIME=False):
        global visited, visitedLock, linkBuffer, bufferLock
        # logging.info('Links Buffer size:{}'.format(len(linkBuffer)))
        # logging.info('visited size:{}'.format(len(visited)))

        try:
            bufferLock.acquire()
            url = linkBuffer.pop()
            bufferLock.release()

            source = urllib.request.urlopen(url).read()
            source = source.decode('utf-8')
            filename = "{}{}.html".format(
                HTML_SAVE_FOLDER, self.getArticleTitle(source))
            if self.getArticleTitle(source) + '.html' in listdir(HTML_SAVE_FOLDER):
                logging.warning('file already exists! will overwrite')
            f = open(filename, 'w')
            f.write(source)
            f.close()
        except KeyError:
            logging.warning('linkBuffer empty')
            return

        allLinks = self.getArticleLinks(source, limit=CRAWLER_WIDTH)

        # logging.info('links_to_add_count:{}'.format(len(allLinks)))
        before = len(allLinks)
        allLinks = list(filter(lambda x: x not in visited, allLinks))
        logging.info('links_filtered:{}'.format(before - len(allLinks)))
        

        # logging.info('links_to_add_count:{}'.format(len(allLinks)))
        for link in allLinks:
            visitedLock.acquire()
            visited.add(link)
            visitedLock.release()

            bufferLock.acquire()
            linkBuffer.add(link)
            # logging.info('buffer_add, size:{}'.format(len(linkBuffer)))
            bufferLock.release()


def main():

    for i in range(THREADS):
        miner = Miner(i)
        miner.setDaemon(True)
        miner.start()

    t0 = time.time()
    while(True):
        delta = time.time() - t0
        count = len(listdir(HTML_SAVE_FOLDER))
        logging.info('speed: {} f/s'.format(count / delta))
        # print('{} files in {} seconds'.format(count, delta))
        time.sleep(1)
if __name__ == '__main__':
    main()
