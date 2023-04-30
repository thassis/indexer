import os
import sys
import resource
import argparse
import pandas
import psutil
import time

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')

from file_writer import get_corpus_jsons, write_partial_index, merge_inverted_lists, create_document_index, clean_file

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
    
    #memory logs
    pid = os.getpid()
    process = psutil.Process(pid)
    memory_usage = process.memory_info().rss
    # print(memory_usage, get_memory_limit_value(), psutil.virtual_memory().total)
    #end of memory logs
    if memory_usage > get_memory_limit_value() * 0.4: #se tiver passando de 40% da memória, precisa liberar para continuar a leitura
        write_partial_index(inverted_list, list_number)
        
        list_number += 1
        inverted_list = {}
        inverted_list.clear()
    elif is_last_doc:
        write_partial_index(inverted_list, list_number)
        
        list_number += 1
        inverted_list = {}
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

def main():
    inverted_list = {}
    print("start")
    clean_file("/document_index.txt")
    clean_file("/term_lexicon.txt")

    start = time.time()
    # df = df.sort_values(by='id', ascending=True)

    it = 0
    # for df in get_corpus_jsons(get_memory_limit_value()):
    #     print("Index: " + str(it) + " " + str(len(df)))
    #     pid = os.getpid()
    #     process = psutil.Process(pid)
    #     memory_usage = process.memory_info().rss
    #     # print(memory_usage, get_memory_limit_value(), psutil.virtual_memory().total)
    #     it+=1
    #     for index, doc in df.iterrows():
    #         doc_id = int(doc['id'])
    #         text = doc['text']
            
    #         words = tokenize(text)
        
    #         indexer(doc_id, words, inverted_list, index == len(df) - 1)
    #         create_document_index(doc_id, words)
    #     print("list: " + str(sys.getsizeof(inverted_list)), "words: " + str(sys.getsizeof(words)), "usage: " + str(memory_usage))

    merge_inverted_lists(get_memory_limit_value())

    end = time.time()
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
