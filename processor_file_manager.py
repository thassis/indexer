import time
import ast
import re

def get_queries(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        string_array = [line.strip() for line in lines]
    return string_array


def get_index(path):
    data_index = {}
    lines_with_error = 0
    start = time.time()

    if path[-1] != '/':
        path += '/'
    with open(path + "index.txt", 'r') as f:
        # iterar sobre as linhas e extrair as palavras e os arrays de documents
        for linha in f:
            # quebra a linha para pegar a termo e depois seus postings
            splited_line = linha.split(':')
            
            processed_word = splited_line[0] 
            documents = []


            # separa os documents em um array
            docs = splited_line[1].split('),')

            for doc in docs:
                doc = doc.replace('\n', '')
                doc = doc.replace(' ', '')

                if doc is not None and type(doc) == str and len(doc) > 0:
                    if doc[-1] != ')':
                        doc = doc + ')'
                try:
                    # transforma o documento numa tupla para salvá-lo
                    doc = doc.replace(",,", ",1),")
                    pattern = r"(\d+),\("
                    replacement = r"\1,1),("
                    updated_data = re.sub(pattern, replacement, doc)
                    updated_data = updated_data.replace('(,', '')

                    if len(updated_data) > 0 and updated_data[0] == ',':
                        updated_data = updated_data[1:]
                    literal_doc = ast.literal_eval(updated_data)
                    documents.append(literal_doc)
                except Exception as e:
                    lines_with_error += 1
                    pass

            # com a string de documents convertida para um array de tuplas, salva no dicionário os documents referentes a cada processed_word da lista invertida
            if documents is not None:
                if processed_word in data_index:
                    if data_index[processed_word] is not None:
                        data_index[processed_word] += documents
                    else:
                        data_index[processed_word] = documents
                else:
                    data_index[processed_word] = documents
        end = time.time()

        # print("Tempo para ler index: ", end - start, 'linhas com erro: ', lines_with_error, len(data_index))

        with open(path + 'new_index.txt', 'w') as file:
            for key, value in data_index.items():
                file.write(f'{key}: {value}\n')
        return data_index


def get_number_of_documents_corpus(path):
    if path[-1] != '/':
        path += '/'
    with open(path + "index.txt", 'r') as file:
        lines = file.readlines()
    return len(lines)


def get_document_index(path):
    if path[-1] != '/':
        path += '/'
    data = {}
    with open(path + "document_index.txt", 'r') as f:
        for line in f:
            doc = line.split(': ')
            doc[1] = int(doc[1])
            if doc[0] in data:
                data[doc[0]] += doc[1]
            else:
                data[doc[0]] = doc[1]
    sum = 0
    n_docs = 0
    for value in data.values():
        sum += value
        n_docs += 1
    return data, sum/n_docs
