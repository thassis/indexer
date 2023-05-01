from file_writer import get_corpus_jsons, write_partial_index, merge_inverted_lists, create_document_index, clean_files, write_output
import os
import sys
import resource
import argparse
import threading
import psutil
import time
import concurrent.futures

import nltk
nltk.download('punkt')
nltk.download('stopwords')
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


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
    # se tiver passando de 40% da memória, precisa liberar para continuar a leitura
    if memory_usage > get_memory_limit_value() * 0.6:
        print("memoria vai estourar")
        write_partial_index(inverted_list, list_number, args.index_path)

        list_number += 1
        inverted_list.clear()
    elif is_last_doc:
        print("ultimo doc")
        write_partial_index(inverted_list, list_number, args.index_path)

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


def process_corpus_chunk(chunk, inverted_list):
    print("veio aqui?")
    for index, doc in chunk.iterrows():
        doc_id = int(doc['id'])
        text = doc['text']
        words = tokenize(text)

        indexer(doc_id, words, inverted_list, index == len(chunk) - 1)
        create_document_index(doc_id, words, args.index_path)


def process_corpus(num_threads):
    inverted_lists = []
    threads = []
    it = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        for chunk in get_corpus_jsons(get_memory_limit_value(), num_threads, args.corpus_path):
            print("INDEX?:: ", it, " --- ", sys.getsizeof(chunk) / MEGABYTE)
            inverted_lists.append({})
            # Executa a função process_chunk em uma thread da pool
            future_indexes = {executor.submit(process_corpus_chunk,
                                              chunk, inverted_lists[it])}
            it += 1
            if(it%2 == 0):
                for future in concurrent.futures.as_completed(future_indexes):
                    pid = os.getpid()
                    process = psutil.Process(pid)
                    memory_usage = process.memory_info().rss
                    print("INDEXER: usage:", memory_usage / MEGABYTE, "MB", get_memory_limit_value() / MEGABYTE, psutil.virtual_memory().total / MEGABYTE)
                    try:
                        print("deu certo")
                    except Exception as exc:
                        print('generated an exception: %s' % (exc))


def main():
    print("start")
    clean_files(args.index_path)

    start = time.time()

    NUM_THREADS = 4

    print("before process")
    process_corpus(NUM_THREADS)
    print("after process")
    pid = os.getpid()
    process = psutil.Process(pid)
    with open(args.index_path + "/log.txt", "a+") as flog:
        flog.write("Termoniou os indexes: " +
                   str(process.memory_info().rss))
        flog.close()

    end = time.time()

    print("******end indexer::", end - start)

    start_merge = time.time()

    merge_inverted_lists(get_memory_limit_value(), args.index_path)

    end_merge = time.time()
    with open(args.index_path + "/log.txt", "a+") as flog:
        flog.write("\nThe end::" + str(end - start))
    flog.close()

    print("MERGE INDEXER endED::", end_merge - start_merge)

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
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
