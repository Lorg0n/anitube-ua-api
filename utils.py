import hashlib
import pickle
from datetime import datetime
from enum import Enum


class LogLevel(Enum):
    MESSAGE = 1
    WARN = 2
    ERROR = 3


def log(msg, status=LogLevel.MESSAGE):
    now = datetime.now()
    formatted_datetime = now.strftime('[%H:%M:%S]')

    prefix = ''
    if status.value == 1:
        prefix = '[M]'
    elif status.value == 2:
        prefix = '[!]'
    elif status.value == 3:
        prefix = '[ERROR]'
    print(f'{formatted_datetime} {prefix} {msg}')


def to_hash(obj):
    obj_bytes = pickle.dumps(obj)
    hash_object = hashlib.md5(obj_bytes)
    hash_hex = hash_object.hexdigest()
    return hash_hex


def time_sort(a, b):
    a_dt = datetime.strptime(a, '%Y-%m-%d %H:%M:%S.%f')
    b_dt = datetime.strptime(b, '%Y-%m-%d %H:%M:%S.%f')
    return a_dt < b_dt