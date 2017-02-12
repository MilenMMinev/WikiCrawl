import pandas as pd
import numpy as np


def build_tfidf(df, article):
    """
    Build a df with tf and idf rating.
    @param df: df with all docs
    @param article: name of the article
    """

    # A series of words and their count
    words_cnt = df[article].dropna()
    page_df = words_cnt.to_frame()
    page_df['tf'] = words_cnt / sum(words_cnt)
    # Number of columns
    docs_cnt = len(list(df))
    terms = page_df.index.values
    # idf(t) = total documents count / count of documents where t appears
    page_df['idf'] = np.vectorize(lambda x:np.log(docs_cnt / float(df.loc[x].count())))(terms)
    page_df['tfidf'] = page_df['tf'] * page_df['idf']
    print(page_df.sort('tfidf'))

def main():
    df = pd.read_csv('../../data/out.csv', index_col = 0)
    build_tfidf(df, 'Equator - Wikipedia')

if __name__ == '__main__':
    main()
