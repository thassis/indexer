import os
import sys
import resource
import argparse
import pandas
import nltk
import psutil
import time

nltk.download('punkt')

from file_writer import get_jsons, write_partial_index 

MEGABYTE = 1024 * 1024

def get_memory_limit_value():
    return args.memory_limit * MEGABYTE

def memory_limit(value):
    limit = get_memory_limit_value()
    print(resource.RLIMIT_AS, limit)
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def indexer(doc_id, words, inverted_list, list_number, is_last_doc):
    for word in words:
        if word not in inverted_list:
            inverted_list[word] = []
        inverted_list[word].append(doc_id)
    
    #memory logs
    print(get_memory_limit_value(), sys.getsizeof(inverted_list))
    process = psutil.Process()
    memory_usage = process.memory_info().rss
    print(f"Memory usage: {memory_usage / (1024 ** 2):.2f} MB")
    #end of memory logs

    if sys.getsizeof(inverted_list) > get_memory_limit_value():
        write_partial_index(inverted_list, list_number)
        
        list_number += 1
        inverted_list.clear()
    elif is_last_doc:
        write_partial_index(inverted_list, list_number)
        
        list_number += 1
        inverted_list.clear()

def tokenize(words):
    return nltk.word_tokenize(words)

def main():
    inverted_list = {}
    doc_index = {}
    term_lexicon = {}

    print("start")

    start = time.time()

    df = get_jsons()

    # df = df.sort_values(by='id', ascending=True)

    print("finished", df)
    
    for index, doc in df.iterrows():
        print("Index: " + str(index) + " " + str(len(df)))
        doc_id = int(doc['id'])
        text = doc['text']
        
        words = tokenize(text)

        indexer(doc_id, words, inverted_list, 0, index == len(df) - 1)
    
    # merge_indexes(inverted_list, doc_index, term_lexicon)

    end = time.time()
    print(end - start)

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
    memory_limit(args.memory_limit)
    try:
        main()
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
