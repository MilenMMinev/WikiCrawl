import urllib.request
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
import math

from os import listdir
from os.path import isfile, join
BEGIN_ID = 0

import time

from constants import *

class SourceNullException(Exception):
     def __str__(self):
        return 'Source is null'

class Article():

    def __init__(self, sourcePath):
        t0 = time.time()
        self.title = sourcePath[len(HTML_SAVE_FOLDER):]
        source = open(sourcePath, 'r').read()
        if len(source) == 0:
            raise SourceNullException()
        self.words = self._getWords(source)
        self._mostFrequentTermCount = max([self.words.count(x) for x in self.words])
        self.df = pd.DataFrame()
        # print('article init:{}'.format(time.time() - t0))

    def loadFromCsv(self, path):
        self.df = pd.read_csv(path, sep=';')
        self.title = path[path.find('articles/') + len('articles/'):len(path) - 4]

        result = list(self.df['words'])

        self.words = result
        self._mostFrequentTermCount = self.df['count'].max()
        self.dfInited = True

    def _getPlainText(self, source):
        soup = BeautifulSoup(source, "lxml")
        return soup.get_text()

    def _getWords(self, source):
        text = self._getPlainText(source)
        result = []
        for word in text.split(" "):
            word = word.lower()
            if len(word) < MAX_VALID_WORD_LENGTH and re.match(r"^[a-zA-Z]+$", word) and not word in BLACKLIST:
                result.append(word)

        return sorted(result)
        # self.words = result

# TF can be evaluated only if the dataframe is in format word : count
    def tf(self, term):
        return self.words.count(term) / self._mostFrequentTermCount


    def idf(self, term, docOccurance, docCount):
        return math.log(docCount / docOccurance)

    def createDataFrame(self):
        if not self.df.empty:
            return self.df        
        df = pd.DataFrame(self.words, columns=['words'])
        df['count'] = df.groupby(['words'])['words'].transform('count')
        df = df.drop_duplicates(['words'])
        self.df = df
        return df

    def fillIdf(self, globalData, docCount):
        self.df['idf'] = pd.Series([self.idf(x, globalData, docCount) for x in self.words])


    def toCsv(self):
        self.df.to_csv('{}{}.csv'.format(ARTICLE_EXPORT_DIR, self.title), sep=';', index=False)

def main():
    # article = Article()
    # article.loadFromUrl('https://en.wikipedia.org/wiki/Food')
    # article.toCsv()

    # a = Article()
    # a.loadFromCsv(ARTICLE_EXPORT_DIR + "Food.csv")
    # a.createDataFrame(tf=True).to_csv('test.csv')

    articles = loadArticles(HTML_SAVE_FOLDER, limit=20, verbose=True)
    print(mergeDocData(articles))



    # print(articles[0].createDataFrame())
    # print(articleWords)
    # print(articles[0].words)


if __name__ == '__main__':
    main()
