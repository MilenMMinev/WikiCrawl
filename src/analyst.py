import time
import logging
import pandas as pd
import multiprocessing as mp
from os import listdir
from os.path import isfile, join

from queue import Queue

from constants import *
from article import Article
from article import SourceNullException

logging.basicConfig(filename='analyst.log', level=logging.INFO)

# article is a


class AnalyzerProcess(mp.Process):
    def __init__(self, queue, files):
        mp.Process.__init__(self)
        self.files = files
        self.queue = queue

    def run(self):
        self.loadArticles(self.files, verbose=True)
        articlesQueue.task_done()

    def loadArticles(self, files, verbose=False):
        cnt = 0
        for f in files:
            cnt += 1
            try:
                begin = time.time()
                article = Article(f)
                logging.info('article_load,{},'.format(time.time() - begin))
                self.queue.put(article)
                logging.info('load_and_put,{},'.format(time.time() - begin))
            except SourceNullException:
                print('skipping {}'.format(f))

        self.queue.join()


class Analyst():
    def __init__(self):
        self.docsDF = pd.DataFrame()
        self.articles = Queue()

    def fillTf(self):
        for article in self.articles:
            df = article.createDataFrame()
            df['tf'] = df['words'].apply(article.tf)
            article.df = df

    def getDocOcc(self, term):
        return int(self.docsDF['doc_occurance'].loc[self.docsDF['words'] == term])

    def fillIdf(self):
        for article in self.articles:
            print('fillIdf {} of {}'.format(
                self.articles.index(article), len(self.articles)))
            df = article.createDataFrame()
            article.df['idf'] = df['words'].apply(
                lambda row: article.idf(row, self.getDocOcc(row), len(self.articles)))
            article.df['tfidf'] = df['tf'] * df['idf']

    def mergeDocData(self):
        result = pd.DataFrame()
        for article in self.articles:
            print('merging {} of {}'.format(article.title, len(self.articles)))
            result = result.append(article.createDataFrame())
        result['doc_occurance'] = result.groupby(
            ['words'])['words'].transform('count')
        result = result.drop('count', 1)
        result = result.drop_duplicates()
        result.to_csv('doc_data.csv', sep=';')
        self.docsDF = result

    def exportArticleData(self, outputDir):
        for a in self.articles:
            a.createDataFrame().to_csv(join(outputDir, a.title))


def main():
    d = HTML_SAVE_FOLDER
    THREADS = 8
    files = [join(d, f) for f in listdir(d) if isfile(join(d, f))]
    cnt = len(files)
    files = [
        files[cnt // THREADS * i:cnt // THREADS * (i + 1)] for i in range(THREADS)]
    for f in files:
        worker = AnalyzerProcess(articlesQueue, f)
        worker.daemon = True
        worker.start()

    beginTime = time.time()
    while True:
        n = articlesQueue.qsize() + 1
        elapsed = time.time() - beginTime
        # logging.info('speed: ,{},'.format(n / elapsed))
        # print('speed: ,{},'.format(n / elapsed))
        time.sleep(1)

if __name__ == '__main__':
    articlesQueue = mp.Manager().Queue()

    main()
