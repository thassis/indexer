import os
import json
import pandas

CORPUS_DIR = '../files/simple_corpus.jsonl'

GENERATED_DIR = "../generated_files/"

PARTIAL_FILE_NAME = "inverted_index"

def get_jsons():
    return pandas.read_json(CORPUS_DIR, lines=True)

def write_partial_index(inverted_list, list_number):
    filename = f'inverted_list_{list_number}.json'
    with open(filename, 'w') as f:
        json.dump(inverted_list, f)
