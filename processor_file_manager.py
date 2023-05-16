import time
import ast


def get_queries(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        string_array = [line.strip() for line in lines]
    return string_array


def get_index(path):
    start = time.time()

    if path[-1] != '/':
        path += '/'
    with open(path + "index.txt", 'r') as f:
        data = {}
        # iterar sobre as linhas e extrair as palavras e os arrays de documentos
        for linha in f:
            try:
                linha = linha.replace(" ", "")

                # quebra a linha para pegar a termo e depois seus postings
                splited_line = linha.split(':[(')

                palavra = splited_line[0]

                documentos = []

                # faz um tratamento da string para converter os valores lidos em um array de tuplas
                splited_line[1] = '[(' + splited_line[1]
                splited_line[1] = splited_line[1].replace("[", "")
                splited_line[1] = splited_line[1].replace("]", "")
                splited_line[1] = splited_line[1].replace(" ", "")
                splited_line[1] = splited_line[1].replace(":", "")
                splited_line[1] = splited_line[1]

                # separa os documentos em um array
                docs = splited_line[1].split('),')

                for doc in docs:
                    doc.replace('\n', '')
                    doc.replace(' ', '')
                    if ')' not in doc:
                        doc += ")"

                    try:
                        # transforma o documento numa tupla para salvá-lo
                        literal_doc = ast.literal_eval(doc)
                        documentos.append(literal_doc)
                    except:
                        # alguns documentos tiveram a formatação incorreta, pois o corpus tinha muitas caracteres que invalidaram a conversão
                        pass

                # com a string de documentos convertida para um array de tuplas, salva no dicionário os documentos referentes a cada palavra da lista invertida
                if documentos is not None:
                    if palavra in data:
                        if data[palavra] is not None:
                            data[palavra] = data[palavra].extend(documentos)
                        else:
                            data[palavra] = documentos
                    else:
                        data[palavra] = documentos
            except IndexError:
                # alguns documentos tiveram a formatação incorreta, pois o corpus tinha muitas caracteres que invalidaram a conversão
                pass
        end = time.time()

        print("Tempo para ler index: ", end - start)

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
