import urllib.request
import urllib.response
import time
import re
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread, Lock

from os import listdir
from constants import *

# amount of links crawler visits per page
CRAWLER_WIDTH = 20

visitQueue = set()
visitQueue.add(ARTICLE_URL)


class Miner(Thread):

    def __init__(self, links, lock):
        Thread.__init__(self)
        self.links = links
        self.lock = lock

    def run(self):
        for l in self.links:
            self.minePage(l)

    def getArticleTitle(self, source):
        return re.findall(r"<title>(.*) - Wikipedia", source)[0]

    def getArticleLinks(self, source, limit=10**6):
        relativeLinks = []
        wikiDomain = "http://wikipedia.org"

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

# saves article text to file
    def minePage(self, url, TIME=False):
        source = urllib.request.urlopen(url).read()
        source = source.decode('utf-8')
        filename = "{}{}.html".format(
            HTML_SAVE_FOLDER, self.getArticleTitle(source))
        # if self.getArticleTitle(source) + '.html' in listdir(HTML_SAVE_FOLDER):
            # print('file already exists! will overwrite')
            # print(self.getArticleTitle(source) + '.html')
            # print(listdir(HTML_SAVE_FOLDER))
        f = open(filename, 'w')
        f.write(source)
        f.close()

        allLinks = self.getArticleLinks(source, limit=CRAWLER_WIDTH)
        global visitQueue
        self.lock.acquire()
        for link in allLinks:
            visitQueue.add(link)
        self.lock.release()

    # def crawl(self, root):
    #     self.minePage(root)
    #     while self.visitQueue:
    #         link = self.visitQueue.get()
    #         if link not in self.visited:
    #             self.minePage(link)
    #     f.close()


def main():
    lock = Lock()
    THREADS = 4

    while True:
        if len(visitQueue) < THREADS:
            miner = Miner(visitQueue, lock)
            miner.setDaemon(True)
            miner.start()
            miner.join()

        else:

        for link in visitQueue:
            work = visitQueue[:2]
            miner = Miner(work, lock)
            miner.setDaemon(True)
            miner.start()

    t0 = time.time()
    while(True):
        delta = time.time() - t0
        count = len(listdir(HTML_SAVE_FOLDER))
        print('speed: {} f/s'.format(count / delta))
        # print('{} files in {} seconds'.format(count, delta))
        time.sleep(1)
if __name__ == '__main__':
    main()
