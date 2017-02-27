import re
import sys
import time
import os
from multiprocessing import Process, Manager
import pandas as pd
from word_blacklist import WORD_BLACKLIST

PAGES_RAW = '../../data/raw'
PAGES_CSV = '../../data/frames'

THREADS_CNT = 4
MIN_WORD_LEN = 3


def get_title(html):
    title_match = re.search(r'<title>(.*?)</title>', html)
    title_str = title_match.group(1)
    return title_str


def _strip_html(html):
    """
    Clean up raw html from tags
    Returns a list of all paragraphs extracted
    """
    # Cut html up until notes and bibliography section
    pos_reference = html.find('<ol class="references">')
    if pos_reference != -1:
        html = html[:pos_reference]

    paragraphs = []

    # Tags we use to find text
    paragraphs += re.findall(r'<p>(.*?)</p>', html)
    paragraphs += re.findall(r'<li>(.*?)</li>', html)

    # Filters to clean text
    paragraphs = list(map(lambda x: re.sub(r'<a.*?>', '', x), paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'</a>', '', x), paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'<.*?/?>', '', x), paragraphs))

    paragraphs = list(map(lambda x: re.sub(r'\[.*\]', '', x), paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'[^\w\s\-]', '', x), paragraphs))
    paragraphs = list(map(lambda x: re.sub(
        r'\w*\-?\d+\-?\w*', '', x), paragraphs))

    paragraphs = list(map(lambda x: re.sub(r'[\n\r\t]+', ' ', x), paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'\s+', ' ', x), paragraphs))

    # Remove empty lists
    paragraphs = filter(lambda x: x, paragraphs)

    paragraphs = list(map(lambda x: x.lower(), paragraphs))
    return paragraphs


def _get_words(html):
    """
    Filter words
    @param paragraphs    A list of clean sentences
    @param min_word_len  Words with length <= min_word_len
                         will be filtered
    @return              A sorted list of tuples of all terms
                         and their occurance couns
    """
    paragraphs = _strip_html(html)
    words = []
    for p in paragraphs:
        # words += p.split(' ')
        words += re.findall(r'[a-zA-Z]{3,}', p)

    words = list(
        filter(lambda x: (x not in WORD_BLACKLIST), words))

    words_cnt = ((x, words.count(x)) for x in set(words))
    return words_cnt


def parse_html(html):
    """
    Clean up raw html from tags
    Returns a pandas df with words
    """

    title = get_title(html)
    words = _get_words(html)
    df = pd.DataFrame(words, columns=['term', title]).set_index(['term'])

    return df


def parse_all_worker(files, out_list):
    """
    Build a list of dataframes out of raw files
    @param files:A list of file paths. Each file
    contains meaningful text only.
    @param out_list: A synchronised object to hold dfs.
    """

    for f in files:
        try:
            f_html = open(f, 'r').read()
        except:
            print('failed to open file: {}'.format(f))

        df = parse_html(f_html)
        out_list.append(df)


def parse_all(files):
    """
    Spawn processes to parse files.
    @return a list of dataframes.
    """
    begin = time.time()
    global THREADS_CNT
    if len(files) < THREADS_CNT:
        THREADS_CNT = 1
    chunk_size = (len(files) // THREADS_CNT) + 1
    file_chunks = [files[i:i + chunk_size]
                   for i in range(0, len(files), chunk_size)]
    dfs_list = Manager().list()
    # Worker processes
    threads = [Process(target=parse_all_worker, args=((file_chunks[i], dfs_list)))
               for i in range(THREADS_CNT)]

    print('parsing {} files in total...'.format(len(files)))
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print('finished in: {}s.'.format(time.time() - begin))
    return list(dfs_list)


def merge_dfs(dfs):
    return dfs


def main(argv):
    assert(len(argv) > 0)
    dfs = parse_all(argv)
    res = pd.concat(dfs, axis=1)

    res.to_csv('../../data/out.csv')

if __name__ == '__main__':
    main(sys.argv[1:])
