import os

CORPUS_DIR = '../files/simple_corpus.jsonl'

GENERATED_DIR = "../generated_files/"

PARTIAL_FILE_NAME = "inverted_index"

def write_partial_list(inverted_list, list_number):
    with open(os.path.join(GENERATED_DIR, PARTIAL_FILE_NAME.join(" " + list_number + ".txt")), "w") as f:
        for word in inverted_list.keys():
            f.write(f"{word}: {inverted_list[word]}\n")