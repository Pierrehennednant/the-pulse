import json
import os
import tempfile


def atomic_write_json(path, data, indent=2):
    dir_ = os.path.dirname(os.path.abspath(path))
    with tempfile.NamedTemporaryFile('w', dir=dir_, delete=False, suffix='.tmp') as tf:
        json.dump(data, tf, indent=indent)
        tmp_path = tf.name
    os.replace(tmp_path, path)
