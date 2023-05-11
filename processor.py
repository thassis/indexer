
from processor_file_manager import get_queries, get_index
import os
import sys
import resource
import argparse
import psutil
import time
import concurrent.futures
import gc
import heapq

import nltk

from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

NUMBER_OF_DOCUMENTS = 10

def daat(query_tokens, index, k):
    results = []
    for target in range(1, len(index)):
        score = 0
        for term in query_tokens:
            postings = index[term]
            for docid in postings:
                if docid == target:
                    score += 1
                    #se já achou o target, entao pode pular para o próximo termo.
                    break
                elif docid > target:
                    #se já passou do target, entao também pode pular para o próximo termo.
                    break
        if score > 0:
            heapq.heappush(results, (score, target))
            if len(results) > k:
                heapq.heappop(results)
    return sorted(results, reverse=True)

def tokenize(text):
    ps = PorterStemmer()

    stop_words = set(stopwords.words('english'))

    tokens = word_tokenize(text.lower())
    words = []
    for token in tokens:
        if token not in stop_words and len(token) != 0 and token != "":
            words.append(ps.stem(token))
    return words

def main():
    queries = get_queries(args.queries_path)
    
    index = get_index(args.index_path) 

    #inicia processamento de cada query
    for query in queries:
        tokens = tokenize(query)
        print(f"QUERY {query}: ", daat(tokens, index, NUMBER_OF_DOCUMENTS))

    print(queries)

    #inicia processamento

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        type=str,
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
