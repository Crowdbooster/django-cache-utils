from hashlib import md5

from django.utils.encoding import smart_text


CONTROL_CHARACTERS = set([chr(i) for i in range(0, 33)])
CONTROL_CHARACTERS.add(chr(127))

PREFIX = '[cached]'


def sanitize_memcached_key(key, max_length=250):
    """ Removes control characters and ensures that key will
        not hit the memcached key length limit by replacing
        the key tail with md5 hash if key is too long.
    """
    key = ''.join([c for c in key if c not in CONTROL_CHARACTERS])
    if len(key) > max_length:
        try:
            from django.utils.encoding import force_bytes
            return md5(force_bytes(key)).hexdigest()
        except ImportError:  # Python 2
            hash = md5(key).hexdigest()
        key = key[:max_length - 33] + '-' + hash
    return key


def serialize(value):
    return smart_text(value)


class Missing:
    def __repr__(self):
        return 'MISSING'


MISSING = Missing()
