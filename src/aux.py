import json

def main():
    partial_json = '{"a": [1,2,3], "b": [1,'
    decoder = json.JSONDecoder(strict=False)
    obj = None
    idx = 0
    while True:
        try:
            obj, idx = decoder.raw_decode(partial_json[idx:])
            # obj contém o objeto JSON lido, idx contém o índice do caractere após o objeto JSON
            if idx == len(partial_json):
                # Todo o JSON válido foi lido
                break
        except json.JSONDecodeError:
            # A string JSON não é válida
            break

    if obj is not None:
        # Todo o JSON válido foi lido
        json_str = json.dumps(obj)
    else:
        # Não foi possível ler todo o JSON válido
        pass
    print(obj)

if __name__ == "__main__":
    main()
