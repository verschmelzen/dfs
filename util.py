import random
import string


class CommandError(Exception):
    pass


ID_SYMBOLS = string.ascii_lowercase + string.digits
ID_LENGTH = 6


def gen_id():
    return ''.join(random.choice(ID_SYMBOLS) for _ in range(6))

