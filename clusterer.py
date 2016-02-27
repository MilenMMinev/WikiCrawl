import pandas as pd
import numpy as np
from constants import *

from os import listdir
from os.path import isfile, join
import time

centers = ['Copper.html','Food.html', 'Arabic.html', 'Earth.html', 'Electron.html']
centers = list(map(lambda x: join(ANALYZED_ARTICLES, x), centers))

begin = time.time()
allWords = list(pd.read_csv(ALL_DOCUMENTS_DATA_PATH, sep=';')['words'])
# print('loaded all words in: {}'.format(time.time() - begin))
def getArticleVector(article):

    tfidfDict = pd.read_csv(article).set_index('words')['tfidf'].to_dict()
    # return pd.read_csv(article)['tfidf']


    return np.array([0 if not x in tfidfDict else tfidfDict[x] for x in allWords])

def similarity(v1, v2):

	return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

def main():
	files = [f for f in listdir(ANALYZED_ARTICLES) if isfile(join(ANALYZED_ARTICLES, f))]
	files = list(map(lambda x: join(ANALYZED_ARTICLES, x), files))

	c = getArticleVector(centers[0])
	begin = time.time()
	for f in files:
		vector = getArticleVector(f)
		print(f,',',similarity(vector, c))
		# norm = np.linalg.norm(vector)
		# print('loading average per article: {}'.format((time.time() - begin) / (1 + files.index(f))))

	# print('loaded {} articles in total: {}'.format(len(files), time.time() - begin))

if __name__ == '__main__':
    main()
