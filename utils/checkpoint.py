import json

def save_json(savings, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            # indent=2 is not needed but makes the file human-readable
            json.dump(savings, f, indent=2)

    except FileNotFoundError:    # FileNotFoundError: [Errno2] No such file or directory: '~'
        path = filename[:filename.rfind('/')]

        if not os.path.exists(path):
            os.makedirs(path)

        with open(filename, 'w', encoding='utf-8') as f:
            # indent=2 is not needed but makes the file human-readable
            json.dump(savings, f, indent=2)

def load_json(saving_list, filename):
    with open(filename, 'r') as f:
        load = json.load(f)

    savings = []

    for i in saving_list:
        savings.append(load[i])

    return tuple(savings)
