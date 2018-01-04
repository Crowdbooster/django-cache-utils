# -*- coding: utf-8 -*-

import logging

from django.core.cache import caches
from django.utils.functional import wraps

from cache_utils.utils import (
    _args_to_unicode,
    _func_info,
    _func_type,
    _get_hashable_args,
    sanitize_memcached_key,
    PREFIX,
)


logger = logging.getLogger("cache_utils")


def cached(timeout, group=None, backend=None,
           fn_key=None,
           key=_args_to_unicode):
    """ Caching decorator. Can be applied to function, method or classmethod.
    Supports bulk cache invalidation and invalidation for exact parameter
    set. Cache keys are human-readable because they are constructed from
    callable's full name and arguments and then sanitized to make
    memcached happy.

    It can be used with or without group_backend. Without group_backend
    bulk invalidation is not supported.

    Wrapped callable gets `invalidate` methods. Call `invalidate` with
    same arguments as function and the result for these arguments will be
    invalidated.
    """

    def _get_key(fn, *args, **kwargs):
        new_args, new_kwargs = _get_hashable_args(_func_type(fn),
                                                  *args, **kwargs)
        if fn_key:
            if callable(fn_key):
                fn_str = str(fn_key(fn))
            else:
                fn_str = str(fn_key)
        else:
            fn_str, _ = _func_info(fn, args)
        args_str = str(key(*new_args, **new_kwargs))
        return sanitize_memcached_key(
            '%s%s(%s)' % (PREFIX, fn_str, args_str)
        )

    if group:
        backend_kwargs = {'group': group}
    else:
        backend_kwargs = {}

    if backend:
        cache_backend = caches[backend]
    else:
        cache_backend = caches['default']

    def _cached(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # try to get the value from cache
            key = _get_key(func, *args, **kwargs)
            value = cache_backend.get(key, **backend_kwargs)

            # in case of cache miss recalculate the value and put it to the cache
            if value is None:
                logger.debug("Cache MISS: %s" % key)
                value = func(*args, **kwargs)
                cache_backend.set(key, value, timeout, **backend_kwargs)
                logger.debug("Cache SET: %s" % key)
            else:
                logger.debug("Cache HIT: %s" % key)

            return value

        def invalidate(*args, **kwargs):
            """
            Invalidates cache result for function called with passed arguments
            """
            key = _get_key(func, *args, **kwargs)
            cache_backend.delete(key, **backend_kwargs)
            logger.debug("Cache DELETE: %s" % key)

        def force_recalc(*args, **kwargs):
            """
            Forces a call to the function & sets the new value in the cache
            """
            key = _get_key(func, *args, **kwargs)
            value = func(*args, **kwargs)
            cache_backend.set(key, value, timeout, **backend_kwargs)
            return value

        def require_cache(*args, **kwargs):
            """
            Only pull from cache, do not attempt to calculate
            """
            key = _get_key(func, *args, **kwargs)
            logger.debug("Require cache %s" % key)
            value = cache_backend.get(key, **backend_kwargs)
            if not value:
                logger.info("Could not find required cache %s" % key)
                raise NoCachedValueException
            return value

        def get_cache_key(*args, **kwargs):
            """ Returns name of cache key utilized """
            key = _get_key(func, *args, **kwargs)
            return key

        wrapper.require_cache = require_cache
        wrapper.invalidate = invalidate
        wrapper.force_recalc = force_recalc
        wrapper.get_cache_key = get_cache_key

        return wrapper
    return _cached


class NoCachedValueException(Exception):
    pass
