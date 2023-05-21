from file_manager import get_corpus_jsons, write_partial_index, merge_inverted_lists, create_document_index, clean_files, write_output, get_next_inverted_list_number
from collections import Counter
import os
import sys
import resource
import argparse
import psutil
import time
import concurrent.futures
import gc
import tracemalloc
import re 

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# se estiver dando erro na maquina na hora de baixar, favor remover esses ifs de download
if not nltk.corpus.stopwords.words('english'):
    nltk.download('stopwords')

if not nltk.sent_tokenize('Hello world.'):
    nltk.download('punkt')


MEGABYTE = 1024 * 1024


def get_memory_limit_value():
    return args.memory_limit * MEGABYTE


def memory_limit():
    limit = get_memory_limit_value()
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


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


def indexer(doc_id, words, inverted_list):
    for word, frequency in words.items():
        if word == "" or word == '' or len(word) == 0:
            pass
        if word not in inverted_list:
            inverted_list[word] = []
        inverted_list[word].append((doc_id, frequency))

    # memory logs
    usage = resource.getrusage(
        resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()
    # get the available memory in bytes
    pid = os.getpid()
    process = psutil.Process(pid)
    memory_usage = process.memory_info().rss

    # print("INDEXER: usage:", memory_usage / MEGABYTE,
    #       "MB", get_memory_limit_value() / MEGABYTE, psutil.virtual_memory().total / MEGABYTE)
    # end of memory logs
    # se tiver passando de 50% da memória, precisa liberar para continuar a leitura
    if memory_usage > get_memory_limit_value() * 0.5:
        write_partial_index(inverted_list, list_number, args.index_path)

        list_number = get_next_inverted_list_number(args.index_path)
        inverted_list.clear()
        gc.collect()
        usage = resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()


def count_words(arr):
    word_counts = Counter()

    for string in arr:
        words = string.split()
        word_counts.update(words)

    return word_counts


def tokenize(text):
    ps = PorterStemmer()

    stop_words = set(stopwords.words('english'))

    tokens = text.split()
    words_counted = count_words(tokens)
    words = []
    for token in tokens:
        filtered_token = filter_word(token)
        if filtered_token not in stop_words and len(filtered_token) != 0 and filtered_token != "":
            words.append((filtered_token, words_counted[filtered_token]))
    result = {}

    for word, value in words:
        nltk_tokens = word_tokenize(word)
        cleaned_tokens = []

        for token in nltk_tokens:
            #Remove caracteres estranhos
            clean_token = re.sub(r'[^a-zA-Z0-9]', '', token)
        
            if clean_token:
                cleaned_tokens.append(ps.stem(clean_token))
                
        for stemmed in cleaned_tokens:
            if stemmed not in result:
                result[stemmed] = 0
            result[stemmed] += value

    return result


def process_corpus_chunk(chunk, inverted_list):
    iteration = 0
    for index, doc in chunk.iterrows():
        doc_id = int(doc['id'])
        text = doc['text']
        words = tokenize(text)

        indexer(doc_id, words, inverted_list)
        create_document_index(doc_id, words, args.index_path)
        iteration += 1
        if iteration == len(chunk):
            list_number = get_next_inverted_list_number(args.index_path)
            write_partial_index(inverted_list, list_number, args.index_path)
            inverted_list.clear()


def process_corpus(num_threads):
    inverted_list = {}
    threads = []
    it = 0
    future_indexes = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
        for chunk in get_corpus_jsons(get_memory_limit_value(), num_threads, args.corpus_path):
            future = executor.submit(
                process_corpus_chunk, chunk, inverted_list)
            future_indexes.append(future)
            pid = os.getpid()
            process = psutil.Process(pid)
            memory_usage = process.memory_info().rss

            # get the available memory in bytes
            usage = resource.getrusage(
                resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()

            it += 1
            if (it % num_threads == 0):
                for future in future_indexes:
                    future.result()
                    # Process the result here


def main():
    # clean_files(args.index_path)
    tracemalloc.start()

    start = time.time()

    NUM_THREADS = 4

    # inicia processo de indexacao
    # process_corpus(NUM_THREADS)

    pid = os.getpid()
    process = psutil.Process(pid)
    with open(args.index_path + "/log.txt", "a+") as flog:
        flog.write("Termoniou os indexes: " +
                   str(process.memory_info().rss))
        flog.close()

    end = time.time()

    merge_inverted_lists(get_memory_limit_value(), args.index_path)

    end_merge = time.time()
    with open(args.index_path + "/log.txt", "a+") as flog:
        flog.write("\nThe end::" + str(end - start))
    flog.close()

    write_output(end_merge - start, args.index_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available'
    )

    parser.add_argument(
        '-c',
        dest='corpus_path',
        action='store',
        required=True,
        type=str,
        help='the path to the corpus file to be indexed.'
    )

    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        type=str,
        help='the path to the directory where indexes should be written.'
    )
    args = parser.parse_args()
    memory_limit()
    try:
        main()
    except MemoryError:
        usage = resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss
        print("ERROR | Usage: ", usage / MEGABYTE)
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
