import os
import math
import json
import pandas
import glob
import heapq
import tempfile

CORPUS_DIR = f'/home/thassis/projects/corpus.jsonl'

CORPUS_SIZE = (os.path.getsize(CORPUS_DIR)) #MB aproximados

num_lines = 0

with open(CORPUS_DIR, 'r') as file:
    for line in file:
        num_lines += 1
file.close()

def get_corpus_jsons(memory_limit):
    print(num_lines)
    line_size = (CORPUS_SIZE / num_lines) * 100
    chunksize = int(math.floor((memory_limit*0.4) / line_size)) #salva 40% da memória para leitura
    print(line_size, chunksize)
    
    if chunksize < 1:
        chunksize = 1
    return pandas.read_json(CORPUS_DIR, lines=True, chunksize=chunksize)

def sort_list(list):
    sorted_list = {}

    for key in sorted(list.keys()):
        sorted_list[key] = list[key]
    return sorted_list

def write_partial_index(inverted_list, list_number):
    filename = f'generated_files/inverted_list_{list_number}.json'

    sorted_list = sort_list(inverted_list)

    with open(filename, 'w') as f:
        f.write('{')
        for i, (key, value) in enumerate(sorted_list.items()):
            f.write(f'"{key}": {value}')
            if i < len(sorted_list) - 1:
                f.write(',')
        f.write('}')

    f.close()

def merge_dicts(d1, d2):
    result = d1.copy()
    for key, value in d2.items():
        if key in result:
            result[key].extend(value)
        else:
            result[key] = value
    return result

def read_jsons(file_list, chunk_size):
    files = [open(file_name, 'r') for file_name in file_list]
    while True:
        chunks = [f.read(chunk_size) for f in files]
        if not any(chunks):
            break
        data = []
        for chunk in chunks:
            print(chunk)
            if(chunk):
                data.append(json.loads(chunk))
        yield data
    for f in files:
        f.close()

def merge_inverted_lists(memory_limit):    
    generated_path = "generated_files"
    output_file = 'output/output.json'
    
    filenames = os.listdir(generated_path)
    filenames = [generated_path + '/' + name for name in filenames if 'inverted_list' in name]
    
    number_of_files = len(filenames)
    chunk_size = math.floor((memory_limit / 2) / number_of_files)
    
    with open(output_file, 'w') as f:
        f.write('{')
        for chunks in read_jsons(filenames, chunk_size):
            merged_data = {}
            for chunk in chunks:
                merged_data = merge_dicts(merged_data, chunk)
            f.write(json.dumps(sort_list(merged_data))[1:-1])
        f.write('}')


    # for filename in glob.glob(os.path.join(generated_path, 'inverted_list*')):
    #     os.remove(filename)
