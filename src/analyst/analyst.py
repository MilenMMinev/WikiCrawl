from scipy.spatial.distance import cosine
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np

import os.path

TFIDF_CSV = '../../data/tfidf.csv'


def get_tfidf(df, article):
    """
    Get tfidf rating
    @param df: df with all docs
    @param article: name of the article
    @return A panda series of term:tfidf
    """

    # A series of words and their count
    words_cnt = df[article].dropna()
    page_df = words_cnt.to_frame()
    page_df['tf'] = words_cnt / sum(words_cnt)
    # Number of columns
    docs_cnt = len(list(df))
    terms = page_df.index.values
    if len(terms) == 0:
        print(article)
    # idf(t) = total documents count / count of documents where t appears
    page_df['idf'] = np.vectorize(lambda x: np.log(
        docs_cnt / float(df.loc[x].count())))(terms)
    page_df['tfidf'] = page_df['tf'] * page_df['idf']
    return page_df['tfidf']


def counts_to_tfidf(df):
    """
    Convert a matrix of terms count to a matrix of tfidf ratings in place.
    Also na -> 0
    """
    for col in df:
        df[col].update(get_tfidf(df, col))
    df.fillna(0, inplace=True)


def main():
    if not os.path.isfile(TFIDF_CSV):
        df = pd.read_csv('../../data/out.csv', index_col=0)
        counts_to_tfidf(df)
        df.to_csv(TFIDF_CSV)
    else:
        df = pd.read_csv(TFIDF_CSV, index_col=0)

    X = df.values
    print(type(X))
    print(X)
    kmeans = KMeans().fit(X)

if __name__ == '__main__':
    main()
