from file_writer import get_corpus_jsons, write_partial_index, merge_inverted_lists, create_document_index, clean_files, write_output, get_next_inverted_list_number
import os
import sys
import resource
import argparse
import threading
import psutil
import time
import concurrent.futures
import gc
import tracemalloc

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

if not nltk.corpus.stopwords.words('english'):
    nltk.download('stopwords')

if not nltk.sent_tokenize('Hello world.'):
    nltk.download('punkt')


MEGABYTE = 1024 * 1024


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
    for word in words:
        if word == "" or word == '' or len(word) == 0:
            pass
        if word not in inverted_list:
            inverted_list[word] = []
        inverted_list[word].append(doc_id)

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
    # se tiver passando de 40% da memória, precisa liberar para continuar a leitura
    if memory_usage > get_memory_limit_value() * 0.5:
        print("memoria vai estourar", memory_usage/MEGABYTE, sys.getsizeof(inverted_list) /
              MEGABYTE, usage / MEGABYTE, get_memory_limit_value() / MEGABYTE)
        write_partial_index(inverted_list, list_number, args.index_path)

        list_number = get_next_inverted_list_number(args.index_path)
        inverted_list.clear()
        gc.collect()
        usage = resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()
        print("after collection", usage)
    elif is_last_doc:
        print("ultimo doc")
        list_number = get_next_inverted_list_number(args.index_path)
        write_partial_index(inverted_list, list_number, args.index_path)
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
    print("corpus chunk!")
    for index, doc in chunk.iterrows():
        doc_id = int(doc['id'])
        text = doc['text']
        words = tokenize(text)

        indexer(doc_id, words, inverted_list, index == len(chunk) - 1)
        create_document_index(doc_id, words, args.index_path)


def process_corpus(num_threads):
    inverted_list = {}
    threads = []
    it = 0
    future_indexes = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_threads) as executor:
        for chunk in get_corpus_jsons(get_memory_limit_value(), num_threads, args.corpus_path):
            print("INDEX?:: ", it, " --- ", sys.getsizeof(chunk) / MEGABYTE)
            future = executor.submit(
                process_corpus_chunk, chunk, inverted_list)
            future_indexes.append(future)
            pid = os.getpid()
            process = psutil.Process(pid)
            memory_usage = process.memory_info().rss

            # get the available memory in bytes
            usage = resource.getrusage(
                resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()

            print("POOL: usage:", memory_usage / MEGABYTE, "MB", "resource usage:", usage / MEGABYTE,
                  "MB\n", get_memory_limit_value() / MEGABYTE, psutil.virtual_memory().total / MEGABYTE)
            it += 1
            if (it % 2 == 0):
                for future in future_indexes:
                    future.result()
                    print("terminou de processar a thread")
                    # Process the result here


def main():
    print("start")
    tracemalloc.start()

    clean_files(args.index_path)

    start = time.time()

    NUM_THREADS = 2

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
        usage = resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss * resource.getpagesize()
        print("ERROR | Usage: ", usage / MEGABYTE)
        # Obter uma lista das estatísticas de alocação de memória atuais
        snapshot = tracemalloc.take_snapshot()

        # Filtrar as estatísticas para incluir apenas as linhas de código que alocam memória para variáveis
        stats = [stat for stat in snapshot.statistics(
            'traceback') if stat.traceback]

        # Agrupar as estatísticas por objeto e somar as alocações de memória para cada objeto
        mem_by_obj = {}
        for stat in stats:
            obj = stat.traceback.format()  # Representação em string da pilha de chamadas
            mem = stat.size / 1024 / 1024  # Converter para KB
            mem_by_obj[tuple(obj)] = mem_by_obj.get(tuple(obj), 0) + mem

        # Ordenar as variáveis pela quantidade de memória usada
        sorted_mem_by_obj = sorted(
            mem_by_obj.items(), key=lambda x: x[1], reverse=True)

        # Imprimir as variáveis e a quantidade de memória usada
        print("[ Top 10 Variáveis ]")
        for obj, mem in sorted_mem_by_obj[:10]:
            print(f"{obj}: {mem:.1f} MB")
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
