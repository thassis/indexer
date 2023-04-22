import os
import pandas

CORPUS_DIR = '../files/simple_corpus.jsonl'

GENERATED_DIR = "../generated_files/"

PARTIAL_FILE_NAME = "inverted_index"

def get_jsons():
    return pandas.read_json(CORPUS_DIR, lines=True)

def write_partial_index(inverted_list, list_number):
    with open(GENERATED_DIR + PARTIAL_FILE_NAME + "_" + str(list_number) + ".txt", "w") as f:
        for word in inverted_list.keys():
            f.write(f"{word}: {inverted_list[word]}\n")