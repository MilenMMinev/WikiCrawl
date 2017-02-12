import pandas as pd
import numpy as np

from os import listdir
from os.path import join

PAGES_CSV = '../../data/frames'

def build_global_data(data_frames):
    """
    Merge all dataframes given by summing the words count
    """
    merged = pd.concat(data_frames, ignore_index=True)
    return merged

def load_csvs(csvs):
    """
    Load a list of files into a list of dfs.
    """
    return [pd.read_csv(x) for x in csvs]


def main():
    files_csv = list(map(lambda x: join(PAGES_CSV, x), listdir(PAGES_CSV)))
    dfs = load_csvs(files_csv)

    glob_data = build_global_data(dfs)
    print(glob_data)

if __name__ == '__main__':
    main()
