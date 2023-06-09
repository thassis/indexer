
from processor_file_manager import get_queries, get_index, get_number_of_documents_corpus, get_document_index
import argparse
import time
import heapq
from concurrent.futures import ThreadPoolExecutor

import re

import nltk

# se estiver dando erro na maquina na hora de baixar, favor remover esses ifs de download
if not nltk.corpus.stopwords.words('english'):
    nltk.download('stopwords')

if not nltk.sent_tokenize('Hello world.'):
    nltk.download('punkt')

from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import math

NUMBER_OF_DOCUMENTS = 10


def get_idf(number_documents_corpus, index, term):
    # idf == log((n+1)/nw)
    return math.log10((number_documents_corpus + 1) / len(index[term]))


def tfidf(index, term, tf, number_documents_corpus):
    idf = get_idf(number_documents_corpus, index, term)

    value_tfidf = tf * idf
    return value_tfidf


def bm25(index, term, tf, number_documents_corpus, doc_id, document_index, avg_terms_document):
    k1 = 1.2
    b = 0.25

    idf = get_idf(number_documents_corpus, index, term)

    # IDF x (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (|d| / avdl)))
    value_bm25 = idf * ((tf * (k1+1)) / (tf + k1*((1-b) +
                                                 b*(document_index[str(doc_id)] / avg_terms_document))))

    return value_bm25

def process_target(target, query_tokens, index, number_documents_corpus, document_index, avg_terms_document):
    score = 0
    try:
        for term in query_tokens:
            postings = index[term]
            if postings is not None:
                for (docid, frequency) in postings:
                    # se o doc_id em questão for o alvo que está sendo analisado, entao faz o processo de ranking
                    if int(docid) == int(target):
                        if args.ranker == "TFIDF": 
                            score += tfidf(index, term, frequency,
                                        number_documents_corpus)
                        else:
                            score += bm25(index, term, frequency,
                                        number_documents_corpus, target, document_index, avg_terms_document)
                        # se já achou o target, entao pode pular para o próximo termo.
                        break
                    elif int(docid) > int(target):
                        # se já passou do target, entao também pode pular para o próximo termo.
                        break
        return (score, target)
    except Exception as e:
        print(e, term, len(index[term]))

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

    tokens = word_tokenize(text)

    cleaned_tokens = []

    for token in tokens:
        #Remove caracteres estranhos
        clean_token = re.sub(r'[^a-zA-Z0-9]', '', token)
        
        if clean_token:
            cleaned_tokens.append(clean_token)

    final_words = []

    for token in cleaned_tokens:
        filtered_token = filter_word(token)
        if filtered_token not in stop_words and len(filtered_token) != 0 and filtered_token != "" and ps.stem(filtered_token) not in final_words:
            final_words.append(ps.stem(filtered_token))
    return final_words


def main():
    queries = get_queries(args.queries_path)

    index = get_index(args.index_path)

    document_index, avg_terms_document = get_document_index(args.index_path)

    # print('avg_terms_document', avg_terms_document, "len index: ", len(index))
    
    number_documents_corpus = get_number_of_documents_corpus(args.index_path)

    start_time_query = time.time()

    # inicia processamento de cada query
    for query in queries:
        tokens = tokenize(query)

        with ThreadPoolExecutor(max_workers=6) as executor:
            # DAAT - pecorre cada documento presente no document index
            results = list(executor.map(process_target, document_index.keys(), [tokens]*len(document_index), [index]*len(document_index), [number_documents_corpus]*len(document_index), [document_index]*len(document_index), [avg_terms_document]*len(document_index)))

        #ordena os elementos num heap e retorna os 10 maiores
        results = heapq.nlargest(NUMBER_OF_DOCUMENTS, results)
        dict_results = []
        for r in results:
            dict_results.append({"ID": r[1], "Score": r[0]})

        output = {"Query": query, "Results": dict_results}
        print(output)

    end_time_query = time.time()

    # print(f"Tempo para processar queries: {end_time_query - start_time_query}")


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
