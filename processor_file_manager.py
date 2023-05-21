import time
import ast
import re

from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

def get_queries(path):
    print("before the problem")
    with open(path, 'r') as f:
        lines = f.readlines()
        string_array = [line.strip() for line in lines]
    print("after reading", string_array)
    return string_array


def get_index(path):
    ps = PorterStemmer()
    start = time.time()

    if path[-1] != '/':
        path += '/'
    with open(path + "index.txt", 'r') as f:
        data = {}
        # iterar sobre as linhas e extrair as palavras e os arrays de documents
        for linha in f:
            try:
                linha = linha.replace(" ", "")

                # quebra a linha para pegar a termo e depois seus postings
                splited_line = linha.split(':[(')

                raw_words = splited_line[0]

                tokens = word_tokenize(raw_words)

                cleaned_tokens = []

                for token in tokens:
                    #Remove caracteres estranhos
                    clean_token = re.sub(r'[^a-zA-Z0-9]', '', token)
                   
                    if clean_token:
                        cleaned_tokens.append(ps.stem(clean_token))
                for processed_word in cleaned_tokens:                        
                    documents = []

                    # faz um tratamento da string para converter os valores lidos em um array de tuplas
                    splited_line[1] = '[(' + splited_line[1]
                    splited_line[1] = splited_line[1].replace("[", "")
                    splited_line[1] = splited_line[1].replace("]", "")
                    splited_line[1] = splited_line[1].replace(" ", "")
                    splited_line[1] = splited_line[1].replace(":", "")
                    splited_line[1] = splited_line[1]

                    # separa os documents em um array
                    docs = splited_line[1].split('),')

                    for doc in docs:
                        doc = doc.replace('\n', '')
                        doc = doc.replace(' ', '')
                        doc = doc.replace('(', '')
                        doc = doc.replace(')', '')                        
                        doc = '(' + doc + ')'

                        try:
                            # transforma o documento numa tupla para salvá-lo
                            literal_doc = ast.literal_eval(doc)
                            documents.append(literal_doc)
                        except:
                            if processed_word == 'appl':
                                print('*****',linha)
                                break
                            # alguns documents tiveram a formatação incorreta, pois o corpus tinha muitas caracteres que invalidaram a conversão
                            # print(doc)
                            # print('eita', linha)
                            pass

                    # com a string de documents convertida para um array de tuplas, salva no dicionário os documents referentes a cada processed_word da lista invertida
                    if documents is not None:
                        if processed_word in data:
                            if data[processed_word] is not None:
                                data[processed_word] = data[processed_word].extend(documents)
                            else:
                                data[processed_word] = documents
                        else:
                            data[processed_word] = documents
            except IndexError:
                # alguns documents tiveram a formatação incorreta, pois o corpus tinha muitas caracteres que invalidaram a conversão
                pass
        end = time.time()

        print("Tempo para ler index: ", end - start)

        with open(path + 'new_index', 'w') as file:
            for key, value in data.items():
                file.write(f'{key}: {value}\n')
        return data


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
