
import os
import sys
import resource
import argparse
import psutil
import time
import concurrent.futures
import gc

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

def main():
    start = time.time()

    pid = os.getpid()
    process = psutil.Process(pid)
    
    with open(args.index_path + "/log.txt", "a+") as flog:
        flog.write("\nThe end::" + str(end - start))
    flog.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        type=int,
        help='the path to an index directory'
    )

    parser.add_argument(
        '-q',
        dest='queries_path',
        action='store',
        required=True,
        type=str,
        help='the path to a file with the list of queries to process.'
    )

    parser.add_argument(
        '-r',
        dest='ranker',
        action='store',
        required=True,
        type=str,
        choices=['TFIDF', 'BM25'],
        help='a string informing the ranking function (either "TFIDF" or "BM25") to be used to score documents for each query.'
    )
    args = parser.parse_args()
    
    main()
