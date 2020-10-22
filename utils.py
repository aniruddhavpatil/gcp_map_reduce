import hashlib
import os
import sys
def hash_function(key, mod):
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % (mod if mod > 0 else 1)

def clear_store():
    files = [
        'intermediate',
        'split',
        'output'
    ]
    curr_dir = os.getcwd()
    store_path = os.path.join(curr_dir, 'simple_key_value_store', 'store')
    dir_list = os.listdir(store_path)
    for f in dir_list:
        for to_delete in files:
            if to_delete in f:
                os.remove(os.path.join(store_path, f))


if __name__ == "__main__":
    if(sys.argv[1] == 'clear'):
        clear_store()
