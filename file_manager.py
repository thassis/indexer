import os
import math
import json
import pandas
import psutil
from ast import literal_eval

MEGABYTE = 1024 * 1024


def get_corpus_jsons(memory_limit, num_threads, corpus_path):
    corpus_size = (os.path.getsize(corpus_path))  # MB aproximados

    num_lines = 0

    with open(corpus_path, 'r') as file:
        for line in file:
            num_lines += 1
    file.close()

    line_size = (corpus_size / num_lines) * 20
    # salva 50% da memória para leitura
    chunksize = int(math.floor(((memory_limit*0.5) / line_size))/num_threads)
    print(chunksize)
    if chunksize < 1:
        chunksize = 1
    return pandas.read_json(corpus_path, lines=True, chunksize=chunksize)


def sort_list(list):
    sorted_list = {}

    for key in sorted(list.keys()):
        sorted_list[key] = list[key]
    return sorted_list


def write_partial_index(inverted_list, list_number, index_path):
    filename = f'{index_path}/inverted_list_{list_number}.txt'

    sorted_list = sort_list(inverted_list)

    with open(filename, 'w') as f:
        for i, (key, value) in enumerate(sorted_list.items()):
            if key == "" or key == '' or len(key) == 0 or not key:
                pass
            string_representation = ', '.join([f'({x}, {y})' for x, y in value])
            f.write(f'{key}: {string_representation}\n')

    f.close()


def create_term_lexicon(list, last_position, index_path):
    with open(index_path + "/term_lexicon.txt", "a+") as f:
        for index, (key, value) in enumerate(list.items()):
            f.write("{}: {}, {}\n".format(
                key, index + last_position, str(len(value))))
    f.close()


def merge_dicts(d1, d2):
    result = d1.copy()
    for key, value in d2.items():
        if key in result:
            result[key] = result[key] + ',' + value
        else:
            result[key] = value
    return result


def read_jsons(file_list, chunk_size):
    files = [open(file_name, 'r') for file_name in file_list]
    while True:
        chunks = []
        for f in files:
            try:
                chunks.append(f.read(math.floor(chunk_size/10)))
            except UnicodeDecodeError:
                # pula dados nao possiveis de ler UTF-8
                pass
        if not any(chunks):
            break
        data = []
        for chunk in chunks:
            if (chunk):
                output_dict = {}
                for line in chunk.splitlines():
                    try:
                        # print(line)
                        key, value_str = line.split(':', 1)
                        output_dict[key.strip()] = value_str
                    except TypeError as e:
                        print("Type error", e)
                        pass
                    except ValueError as e:
                        pass
                    except SyntaxError as e:
                        print("Syntax error>", e)
                        # Linha inválida ou incompleta, ignorar
                        pass
                data.append(output_dict)
        yield data

    for f in files:
        f.close()


def merge_inverted_lists(memory_limit, index_path):
    output_file = index_path + '/index.txt'

    filenames = os.listdir(index_path)
    filenames = [index_path + '/' +
                 name for name in filenames if 'inverted_list' in name]

    number_of_files = len(filenames)
    chunk_size = math.floor((memory_limit / 10) / number_of_files)

    last_position = 0
    with open(output_file, 'w') as f:
        for chunks in read_jsons(filenames, chunk_size):
            merged_data = {}
            for chunk in chunks:
                merged_data = merge_dicts(merged_data, chunk)
            sorted_data = sort_list(merged_data)
            for key, value in sorted_data.items():
                f.write(key + ":" + str(value) + "\n")

            pid = os.getpid()
            process = psutil.Process(pid)

            with open(index_path + "/log.txt", "a+") as flog:
                flog.write("\nTerminou merge: " + str(last_position) +
                           "|" + str(process.memory_info().rss))
            flog.close()
            create_term_lexicon(sorted_data, last_position, index_path)
            last_position += len(sorted_data)


def create_document_index(doc_id, words, index_path):
    with open(index_path + "/document_index.txt", "a+") as f:
        f.write("{}: {}\n".format(doc_id, str(len(words))))
    f.close()


def clean_files(index_path):
    for filename in os.listdir(index_path):
        file_path = os.path.join(index_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def write_output(elapsed_time, index_path):
    index_size = os.path.getsize(index_path + "/index.txt")

    filenames = os.listdir(index_path)
    filenames = [index_path + '/' +
                 name for name in filenames if 'inverted_list' in name]

    number_of_lists = len(filenames)

    total_size = sum(os.path.getsize(filename) for filename in filenames)

    average_size = (total_size / number_of_lists) / MEGABYTE

    data = {
        "Index Size": index_size / MEGABYTE,
        "Elapsed Time": elapsed_time,
        "Number of lists": number_of_lists,
        "Average List Size": average_size
    }

    with open("output.json", "w") as f_out:
        json.dump(data, f_out)

    print(data)

    f_out.close()


def get_next_inverted_list_number(directory):
    files = os.listdir(directory)
    inverted_list_files = [f for f in files if f.startswith('inverted_list_')]
    if not inverted_list_files:
        return 0
    sorted_files = []
    for f in inverted_list_files:
        last_number = int(f.split('.')[0].split('_')[-1])
        sorted_files.append(last_number)
    sorted_files = sorted(sorted_files)
    last_number = sorted_files[-1]
    return last_number + 1
