import pandas
import time

def get_queries(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        string_array = [line.strip() for line in lines]
    return string_array
    
def get_index(path):
    start = time.time()

    with open(path, 'r') as f:
        linhas = f.readlines()

    data = {}

    # iterar sobre as linhas e extrair as palavras e os arrays de documentos
    for linha in linhas:
        aux = linha.strip().split(': ')
        palavra = aux[0]
        documentos = eval(aux[1])
        data[palavra] = documentos
    
    end = time.time()

    print("Tempo para ler index: ", end - start)
    return data