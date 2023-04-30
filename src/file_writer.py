import os
import math
import json
import pandas
import glob
import heapq
import psutil

CORPUS_DIR = f'files/output_file_aa.jsonl'

GENERATED_DIR = "generated_files"

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
    print("list number:::", list_number)
    filename = f'generated_files/inverted_list_{list_number}.txt'

    sorted_list = sort_list(inverted_list)

    with open(filename, 'w') as f:
        for i, (key, value) in enumerate(sorted_list.items()):
            if key == "" or key == '' or len(key) == 0 or not key:
                pass
            f.write(f'{key}: {value}\n')

    f.close()

def create_term_lexicon(list):
    with open(GENERATED_DIR + "/term_lexicon.txt", "a+") as f:
        for key, value in list.items():
            f.write("{}: {}\n".format(key, str(len(value))))
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
        pid = os.getpid()
        process = psutil.Process(pid)
        memory_usage = process.memory_info().rss
        mem = psutil.virtual_memory()
        available_mem = mem.available / (1024 * 1024) # convert to MB
        print(f"Available memory: {available_mem:.2f} MB")
        print(memory_usage)
        print("opened all files", chunk_size)
        chunks = [f.read(math.floor(chunk_size/10)) for f in files]
        print("we still have the chunk")
        if not any(chunks):
            break
        data = []
        for chunk in chunks:
            if(chunk):
                output_dict = {}
                for line in chunk.splitlines():
                    try:
                        key, value_str = line.split(':', 1)
                        value = json.loads(value_str.strip())
                        output_dict[key.strip()] = value
                    except (ValueError, json.JSONDecodeError):
                        # Linha inválida ou incompleta, ignorar
                        pass
                data.append(output_dict)
        yield data
    for f in files:
        f.close()

def merge_inverted_lists(memory_limit):   
    print("---------------STARTED MERGING---------------") 
    output_file = GENERATED_DIR + '/index.json'
    
    filenames = os.listdir(GENERATED_DIR)
    filenames = [GENERATED_DIR + '/' + name for name in filenames if 'inverted_list' in name]
    
    number_of_files = len(filenames)
    chunk_size = math.floor((memory_limit / 10) / number_of_files)
    
    print("unitil here is ok", chunk_size, filenames)
    with open(output_file, 'w') as f:
        f.write('{')
        for chunks in read_jsons(filenames, chunk_size):
            print("here is still ok")
            merged_data = {}
            for chunk in chunks:
                merged_data = merge_dicts(merged_data, chunk)
            sorted_data = sort_list(merged_data)
            f.write(json.dumps(sorted_data)[1:-1])
            create_term_lexicon(sorted_data)

            pid = os.getpid()
            process = psutil.Process(pid)
            memory_usage = process.memory_info().rss
            print(memory_usage)
    
        f.write('}')


    # for filename in glob.glob(os.path.join(GENERATED_DIR, 'inverted_list*')):
    #     os.remove(filename)

def create_document_index(doc_id, words):
    with open(GENERATED_DIR + "/document_index.txt", "a+") as f:
        f.write("{}: {}\n".format(doc_id, str(len(words))))
    f.close()

def clean_file(name):
    with open(GENERATED_DIR + name, "w") as f:
        pass
    f.close()