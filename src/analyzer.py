import re
from word_blacklist import BLACKLIST
import time

import numpy as np

ARTICLES_PATH = "articles/"

MAX_VALID_WORD_LENGTH = 20
MIN_VALID_WORD_LENGTH = 1


class Analyzer():
    def __init__(self):
        # A list of articles read from file
        self.rawArticles = []
        # A list of words_count_dict derived from rawArticles
        self.reducedArticles = []
        self.exportedArticleFileCounter = 0

    def loadArticles(self, toIdx, fromIdx=0):
        for i in range(toIdx):
            articleFile = open(ARTICLES_PATH + str(i), 'r')
            content = articleFile.read()
            self.rawArticles.append(content)

    def idf(self, word):
        wordOccurances = 0

        for r in self.reducedArticles:
            if word in r:
                wordOccurances = wordOccurances + 1
        ratio = wordOccurances / len(self.reducedArticles)
        return np.log(ratio)

    def tfidf(self, word, wordsCount):
        return self.tf(word, wordsCount) * self.idf(word)

# Distance index between wordsCount x and y
    def distance(self, x, y):
        distance = 0
        for word in x.keys():
            if word in y.keys():
                pass


def main():
    timeStamp = time.clock()

    analyzer = Analyzer()
    analyzer.loadArticles(30)
    analyzer.reduceArticles()

    for rArticle in analyzer.reducedArticles:
        analyzer.exportWords(rArticle)

if __name__ == '__main__':
    main()
