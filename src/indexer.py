import os
import sys
import resource
import argparse
import pandas
import nltk
import psutil
import time

MEGABYTE = 1024 * 1024

def get_memory_limit_value():
    return args.memory_limit * MEGABYTE

def memory_limit(value):
    limit = get_memory_limit_value()
    print(resource.RLIMIT_AS, limit)
    # resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def indexer(doc_id, words, inverted_list, doc_index, term_lexicon):
    for word in words:
        if word not in inverted_list:
            inverted_list[word] = []
        inverted_list[word].append(doc_id)
    
    doc_index[doc_id] = len(words)
    
    for word in set(words):
        if word not in term_lexicon:
            term_lexicon[word] = 0
        term_lexicon[word] += 1

    print(get_memory_limit_value(), sys.getsizeof(inverted_list) + sys.getsizeof(doc_index) + sys.getsizeof(term_lexicon))
    process = psutil.Process()
    memory_usage = process.memory_info().rss

    print(f"Memory usage: {memory_usage / (1024 ** 2):.2f} MB")

    with open(os.path.join(GENERATED_DIR, "doc_index.txt"), "w") as f:
        for doc_id in doc_index.keys():
            f.write(f"{doc_id}: {doc_index[doc_id]}\n")
    
    with open(os.path.join(GENERATED_DIR, "term_lexicon.txt"), "w") as f:
        for word in term_lexicon.keys():
            f.write(f"{word}: {term_lexicon[word]}\n")

    if sys.getsizeof(inverted_list) + sys.getsizeof(doc_index) + sys.getsizeof(term_lexicon) > get_memory_limit_value():
        write_index(inverted_list, doc_index, term_lexicon)
        
        # Clear the temporary index
        inverted_list.clear()
        # doc_index.clear()
        # term_lexicon.clear()

def tokenize(words):
    return nltk.word_tokenize(words)

def main():
    inverted_list = {}
    doc_index = {}
    term_lexicon = {}

    print("start")

    start = time.time()


    df = get_jsons()

    df = df.sort_values(by='id', ascending=True)

    print("finished", df)
    
    for index, doc in df.iterrows():
        doc_id = int(doc['id'])
        text = doc['text']
        
        words = tokenize(text)

        indexer(doc_id, words, inverted_list, doc_index, term_lexicon)
    
    write_index(inverted_list, doc_index, term_lexicon)

    end = time.time()
    print(end - start)

def get_jsons():
    return pandas.read_json(CORPUS_DIR, lines=True)

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


# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation