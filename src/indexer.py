from file_writer import get_corpus_jsons, write_partial_index, merge_inverted_lists, create_document_index, clean_file
import os
import sys
import resource
import argparse
import threading
import psutil
import time

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')


MEGABYTE = 1024 * 1024

list_number = 0


def get_memory_limit_value():
    return args.memory_limit * MEGABYTE


def memory_limit():
    limit = get_memory_limit_value()
    print(resource.RLIMIT_AS, limit)
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


def indexer(doc_id, words, inverted_list, is_last_doc):
    global list_number

    for word in words:
        if word == "" or word == '' or len(word) == 0:
            pass
        if word not in inverted_list:
            inverted_list[word] = []
        inverted_list[word].append(doc_id)

    # memory logs
    pid = os.getpid()
    process = psutil.Process(pid)
    memory_usage = process.memory_info().rss
    # print("INDEXER: usage:", memory_usage / MEGABYTE,
    #       "MB", get_memory_limit_value() / MEGABYTE, psutil.virtual_memory().total / MEGABYTE)
    # end of memory logs
    # se tiver passando de 40% da memória, precisa liberar para continuar a leitura
    if memory_usage > get_memory_limit_value() * 0.8:
        print("memoria vai estourar")
        write_partial_index(inverted_list, list_number)

        list_number += 1
        inverted_list.clear()
    elif is_last_doc:
        print("ultimo doc")
        write_partial_index(inverted_list, list_number)

        list_number += 1
        inverted_list.clear()


def tokenize(text):
    ps = PorterStemmer()

    stop_words = set(stopwords.words('english'))

    tokens = word_tokenize(text.lower())
    words = []
    for token in tokens:
        filtered_token = filter_word(token)
        if filtered_token not in stop_words and len(filtered_token) != 0 and filtered_token != "":
            words.append(ps.stem(filtered_token))
    return words


def process_corpus_chunk(chunk, indexer, inverted_list):
    for index, doc in chunk.iterrows():
        doc_id = int(doc['id'])
        text = doc['text']
        words = tokenize(text)

        indexer(doc_id, words, inverted_list, index == len(chunk) - 1)
        create_document_index(doc_id, words)


def process_corpus(num_threads):
    inverted_lists = []
    threads = []
    it = 0
    for chunk in get_corpus_jsons(get_memory_limit_value(), num_threads):
        inverted_lists.append({})
        print("Index: " + str(it) + " " + str(len(chunk)))
        thread = threading.Thread(
            target=process_corpus_chunk, args=(chunk, indexer, inverted_lists[it]))
        print("after procressing")
        threads.append(thread)
        thread.start()
        print("after start")
        it += 1

        # limita o número de threads em execução
        while len(threads) >= num_threads:
            threads = [t for t in threads if t.is_alive()]

    # espera as threads restantes terminarem a execução
    for thread in threads:
        thread.join()


def main():
    print("start")
    clean_file("/document_index.txt")
    clean_file("/term_lexicon.txt")

    start = time.time()

    NUM_THREADS = 4

    process_corpus(NUM_THREADS)

    pid = os.getpid()
    process = psutil.Process(pid)
    with open("log.txt", "a+") as flog:
        flog.write("Termoniou os indexes: " +
                   str(process.memory_info().rss))
        flog.close()

    merge_inverted_lists(get_memory_limit_value())

    end = time.time()
    with open("log.txt", "a+") as flog:
        flog.write("\nThe end::" + str(end - start))
    flog.close()

    print("end::", end - start)


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
    args = parser.parse_args()
    memory_limit()
    try:
        main()
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
