import pandas as pd
import numpy as np

from os import listdir
from os.path import join

PAGES_CSV = '../../data/frames'


def sum_merge_df(dfs):
    """
    merge dataframes by adding all terms
    and summing their occurance count
    """
    df_result = pd.concat(dfs)
    df_result = df_result.groupby(['term'], as_index=False)
    print(df_result)
    df_result = df_result.agg(np.sum)

    return df_result


def build_global_data(data_frames):
    """
    Merge all dataframes given by summing the words count
    """
    merged = sum_merge_df(data_frames)
    # print(merged)
    merged.to_csv('out.csv')

    pass


def main():
    files_csv = list(map(lambda x: join(PAGES_CSV, x), listdir(PAGES_CSV)))
    dfs = [pd.read_csv(x) for x in files_csv]
    glob_data = build_global_data(dfs)


if __name__ == '__main__':
    main()
