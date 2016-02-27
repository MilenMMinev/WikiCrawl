from article import Article
import pandas as pd
import numpy as np

class ArticleExporter():
    def __init__(self):
        self.globalDataCsv = ''
        pass

    def exportToPandas(self, article):
        frameKeys = np.unique(article.words)
        d = {
            'words': frameKeys,
            'count': [article.words.count(x) for x in frameKeys],
            'tf': [article.tf(x) for x in frameKeys]
        }
        df = pd.DataFrame(d, columns=['words', 'count', 'tf'])
        return df

    def exportToCsv(self, article):
        self.exportToPandas(article).to_csv("articles/" + str(article.title[0]) + ".csv", sep=";", index=False)

    def loadArticleData(self, csv):
        return pd.read_csv(csv, sep=';')
        pd.read_csv

    def getGlobalData(self, articlesFolder='data/articles/'):

        if self.globalDataCsv != '':
            return pd.read_csv(self.globalDataCsv, sep=';')

        articlePaths = [f for f in listdir(articlesFolder) if isfile(join(articlesFolder, f))]
        # globalData = {'words':[],'count':[]}
        allWords = pd.DataFrame()
        for article in articlePaths:
            df = self.loadArticleData(join(articlesFolder, article))
            allWords = pd.concat([allWords,df])

        uniqueWords = allWords[['words', 'count']].groupby('words').count()

        print(uniqueWords)

        uniqueWords.to_csv('data/all_words.csv', sep=';')
        self.globalDataCsv = 'data/all_words.csv'
        return uniqueWords

def main():
    articleExporter = ArticleExporter()
    # print(articleExporter.loadArticleData('articles/Vegetable.csv'))
    articleExporter.getGlobalData("articles")


if __name__ == '__main__':
    main()
