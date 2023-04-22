import os
import json
import pandas

CORPUS_DIR = f'files/simple_corpus.jsonl'

def get_jsons():
    return pandas.read_json(CORPUS_DIR, lines=True)

def write_partial_index(inverted_list, list_number):
    filename = f'generated_files/inverted_list_{list_number}.json'
    with open(filename, 'w') as f:
        json.dump(inverted_list, f)
