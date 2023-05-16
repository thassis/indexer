import time
import ast


def get_queries(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        string_array = [line.strip() for line in lines]
    return string_array


def get_index(path):
    start = time.time()

    if path[-1] != '/': path += '/'
    with open(path + "index.txt", 'r') as f:
        data = {}
        # iterar sobre as linhas e extrair as palavras e os arrays de documentos
        for linha in f:
            try:
                # linha = linha.replace("[", "")
                # linha = linha.replace("]", "")
                linha = linha.replace(" ", "")
                aux = linha.split(':[(')
                # if len(aux) == 1:
                #     aux = linha.split(": [(")
                aux[1] = '[(' + aux[1]
                palavra = aux[0]
                documentos = []
                aux[1] = aux[1].replace("[", "")
                aux[1] = aux[1].replace("]", "")
                aux[1] = aux[1].replace(" ", "")
                aux[1] = aux[1].replace(":", "")
                # aux[1] = aux[1].replace(":", "")
                aux[1] = aux[1]
                docs = aux[1].split('),')
                # print(docs)
                for doc in docs:
                    doc.replace('\n', '')
                    doc.replace(' ', '')
                    # print(doc)
                    if ')' not in doc:
                        doc += ")"
                    try:
                        literal_doc = ast.literal_eval(doc)
                        documentos.append(literal_doc)
                    except:
                        # print('**' + doc + '++', doc[-1])
                        pass
                # print(len(documentos)
                if documentos is not None:
                    if palavra in data:
                        if data[palavra] is not None:
                            data[palavra] = data[palavra].extend(documentos)
                        else:
                            data[palavra] = documentos
                    else:
                        data[palavra] = documentos
                # except SyntaxError:
                #     print('name error::' + linha)
                #     break
            except IndexError:
                # print('index error', linha)
                pass
            # except:
            # print('deu erro aqui', aux[0])
            # pass
        end = time.time()

        print("Tempo para ler index: ", end - start)
        return data


def get_number_of_documents_corpus(path):
    if path[-1] != '/': path += '/'
    with open(path + "index.txt", 'r') as file:
        lines = file.readlines()
    return len(lines)

def get_document_index(path):
    if path[-1] != '/': path += '/'
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