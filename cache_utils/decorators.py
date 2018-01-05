# -*- coding: utf-8 -*-

import logging

from django.core.cache import caches
from django.utils.functional import wraps

from cache_utils.utils import (
    serialize,
    sanitize_memcached_key,
    PREFIX,
)
import six


logger = logging.getLogger("cache_utils")


def default_key(*args, **kwargs):
    return args, kwargs


def default_fn_key(func):
    return ".".join([func.__module__, func.__qualname__])


def legacy_key(*args, **kwargs):
    """Used to generate the same cache keys as previous versions
    of django-cache-utils"""
    k = ''
    if args:
        k += serialize(args)
    if kwargs:
        k += serialize(kwargs)
    return k


def legacy_fn_key(func):
    """Used to generate the same function cache key as previous
    versions of django-cache-utils"""
    lineno = ":%s" % six.get_function_code(func).co_firstlineno
    return ".".join([func.__module__, func.__name__]) + lineno


def cached(timeout, group=None, backend=None,
           fn_key=default_fn_key,
           key=default_key):
    """ Caching function decorator.
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

    if group:
        backend_kwargs = {'group': group}
    else:
        backend_kwargs = {}

    if backend:
        cache_backend = caches[backend]
    else:
        cache_backend = caches['default']

    def _cached(func):
        fn_key_str = str(
            fn_key(func)
            if callable(fn_key)
            else fn_key
        )

        if not fn_key_str:
            raise ValueError('fn_key must be non-empty')

        def _get_key(*args, **kwargs):
            args_str = serialize(key(*args, **kwargs))
            return sanitize_memcached_key(
                '%s%s(%s)' % (PREFIX, fn_key_str, args_str)
            )

        @wraps(func)
        def wrapper(*args, **kwargs):
            # try to get the value from cache
            key = _get_key(*args, **kwargs)
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
            key = _get_key(*args, **kwargs)
            cache_backend.delete(key, **backend_kwargs)
            logger.debug("Cache DELETE: %s" % key)

        def force_recalc(*args, **kwargs):
            """
            Forces a call to the function & sets the new value in the cache
            """
            key = _get_key(*args, **kwargs)
            value = func(*args, **kwargs)
            cache_backend.set(key, value, timeout, **backend_kwargs)
            return value

        def require_cache(*args, **kwargs):
            """
            Only pull from cache, do not attempt to calculate
            """
            key = _get_key(*args, **kwargs)
            logger.debug("Require cache %s" % key)
            value = cache_backend.get(key, **backend_kwargs)
            if not value:
                logger.info("Could not find required cache %s" % key)
                raise NoCachedValueException
            return value

        def get_cache_key(*args, **kwargs):
            """ Returns name of cache key utilized """
            key = _get_key(*args, **kwargs)
            return key

        wrapper.require_cache = require_cache
        wrapper.invalidate = invalidate
        wrapper.force_recalc = force_recalc
        wrapper.get_cache_key = get_cache_key

        return wrapper
    return _cached


class NoCachedValueException(Exception):
    pass
