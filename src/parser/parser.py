import re
import sys
import time
import os
from multiprocessing import Process
import pandas as pd
from word_blacklist import WORD_BLACKLIST

PAGES_RAW = '../../data/raw'
PAGES_CSV = '../../data/frames'

THREADS_CNT = 4
"""
Provide a function to parse pages and create pandas dataframes.
"""

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
    paragraphs = list(map(lambda x: re.sub(r'<a.*?>', '', x) ,paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'</a>', '', x) ,paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'<.*?/?>', '', x) ,paragraphs))

    paragraphs = list(map(lambda x: re.sub(r'\[.*\]', '', x) ,paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'[^\w\s\-]', '', x) ,paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'\w*\-?\d+\-?\w*', '', x) ,paragraphs))

    paragraphs = list(map(lambda x: re.sub(r'[\n\r\t]+', ' ', x) ,paragraphs))
    paragraphs = list(map(lambda x: re.sub(r'\s+', ' ', x) ,paragraphs))

    # Remove empty lists
    paragraphs = filter(lambda x: x, paragraphs)

    paragraphs = list(map(lambda x: x.lower(), paragraphs))
    return paragraphs

def _get_words(paragraphs, min_word_len):
    """
    Filter words
    @param paragraphs    A list of clean sentences
    @param min_word_len  Words with length <= min_word_len
                         will be filtered
    @return              A sorted list of tuples of all terms
                         and their occurance couns
    """
    words = []
    for p in paragraphs:
        words += p.split(' ')

    words = list(filter(lambda x: x not in WORD_BLACKLIST, words))
    words = list(filter(lambda x: len(x) > min_word_len, words))

    words_cnt = ((x, words.count(x)) for x in set(words))
    return words_cnt

def parse_html(html, min_word_len=3):

    """
    Clean up raw html from tags
    Returns a pandas df with words
    """
    title = get_title(html)
    paragraphs = _strip_html(html)
    words = _get_words(paragraphs, min_word_len)
    df = pd.DataFrame(words, columns=['term', 'cnt'])

    return df

def parse_all(files):
    for f in files:
        f_html = open(f, 'r').read()
        # only the filename
        f_name = re.sub(r'.*/', '', f)
        df = parse_html(f_html)
        df.to_csv(os.path.join(PAGES_CSV, get_title(f_html) + '.csv'))

def main(argv):
    begin = time.time()

    assert(os.path.isdir(argv[0]))

    files = list(map(lambda x: os.path.join(argv[0], x), os.listdir(argv[0])))
    chunk_size = len(files) // THREADS_CNT
    chunk_size += 1

    file_chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    # Worker processes
    threads = [Process(target=parse_all, args=((file_chunks[i], )))
               for i in range(THREADS_CNT)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print('finished in: {}s.'.format(time.time() - begin))

if __name__ == '__main__':
    main(sys.argv[1:])
