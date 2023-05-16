
from processor_file_manager import get_queries, get_index, get_number_of_documents_corpus, get_document_index
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
import math

NUMBER_OF_DOCUMENTS = 10


def tfidf(index, term, tf, number_documents_corpus):

    # idf == log(n+1)/nw
    idf = math.log10(number_documents_corpus + 1) * len(index[term])

    value_tfidf = tf * idf
    return value_tfidf


def bm25(index, term, tf, number_documents_corpus, doc_id, document_index, avg_terms_document):
    k1 = 1.5
    b = 0.5
    idf = math.log10(number_documents_corpus + 1) * len(index[term])

    # IDF x (f * (k1 + 1)) / (f + k1 * (1 - b + b * (Ld / Lavg)))
    value_bm25 = idf * (tf * (k1+1)) / (tf + k1*((1-b) + b*(document_index[str(doc_id)] / avg_terms_document)))

    return value_bm25


def daat(query_tokens, index, k, number_documents_corpus, document_index, avg_terms_document):
    results = []
    for target in range(1, len(index)):
        score = 0
        for term in query_tokens:
            postings = index[term]
            if postings is not None:
                for (docid, frequency) in postings:
                    if docid == target:
                        if args.ranker == "TFIDF":
                            score += tfidf(index, term, frequency,
                                        number_documents_corpus)
                        else:
                            score += bm25(index, term, frequency,
                                        number_documents_corpus, target, document_index, avg_terms_document)
                        # se já achou o target, entao pode pular para o próximo termo.
                        break
                    elif docid > target:
                        # se já passou do target, entao também pode pular para o próximo termo.
                        break
        if score > 0:
            heapq.heappush(results, (score, target))
            if len(results) > k:
                heapq.heappop(results)
    results = sorted(results, reverse=True)
    dict_results = []
    for r in results:
        dict_results.append({"ID": r[1], "Score": r[0]})
    return dict_results


def filter_word(word):
    if "'" in word:
        word = word.replace("'", "")
    if '"' in word:
        word = word.replace('"', "")
    if '\\' in word:
        word = word.replace("\\", "")
    if ' ' in word:
        word = word.replace(" ", "")
    return word


def tokenize(text):
    ps = PorterStemmer()

    stop_words = set(stopwords.words('english'))

    tokens = text.split()
    words = []
    for token in tokens:
        filtered_token = filter_word(token)
        if filtered_token not in stop_words and len(filtered_token) != 0 and filtered_token != "" and ps.stem(filtered_token) not in words:
            words.append(ps.stem(filtered_token))
    return words


def main():
    queries = get_queries(args.queries_path)

    index = get_index(args.index_path)

    document_index, avg_terms_document = get_document_index(args.index_path)

    print('avg_terms_document', avg_terms_document, "len index: ", len(index))
    number_documents_corpus = get_number_of_documents_corpus(args.index_path)

    # inicia processamento de cada query
    for query in queries:
        tokens = tokenize(query)

        result = daat(tokens, index,
              NUMBER_OF_DOCUMENTS, number_documents_corpus, document_index, avg_terms_document)
        output = {"Query": query, "Results": result}
        print(output)
    # inicia processamento


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
