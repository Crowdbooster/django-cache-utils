from unittest import TestCase

from django.core.cache import cache

from cache_utils.decorators import (
    cached,
    legacy_key,
    legacy_fn_key,
)
from cache_utils.utils import (
    sanitize_memcached_key,
)


class SanitizeTest(TestCase):

    def test_sanitize_keys(self):
        key = u"12345678901234567890123456789012345678901234567890"
        self.assertTrue(len(key) >= 40)
        key = sanitize_memcached_key(key, 40)
        self.assertTrue(len(key) <= 40)


class ClearMemcachedTest(TestCase):

    def tearDown(self):
        cache._cache.flush_all()

    def setUp(self):
        cache._cache.flush_all()


class InvalidationTest(ClearMemcachedTest):

    def test_group_invalidation(self):
        cache.set('vasia', 'foo', 60, group='names')
        cache.set('petya', 'bar', 60, group='names')
        cache.set('red', 'good', 60, group='colors')

        self.assertEqual(cache.get('vasia', group='names'), 'foo')
        self.assertEqual(cache.get('petya', group='names'), 'bar')
        self.assertEqual(cache.get('red', group='colors'), 'good')

        cache.invalidate_group('names')
        self.assertEqual(cache.get('petya', group='names'), None)
        self.assertEqual(cache.get('vasia', group='names'), None)
        self.assertEqual(cache.get('red', group='colors'), 'good')

        cache.set('vasia', 'foo', 60, group='names')
        self.assertEqual(cache.get('vasia', group='names'), 'foo')

    def test_func_invalidation(self):
        self.call_count = 0

        @cached(60)
        def my_func(a, b):
            self.call_count += 1
            return self.call_count

        self.assertEqual(my_func(1, 2), 1)
        self.assertEqual(my_func(1, 2), 1)
        self.assertEqual(my_func(3, 2), 2)
        self.assertEqual(my_func(3, 2), 2)
        my_func.invalidate(3, 2)
        self.assertEqual(my_func(1, 2), 1)
        self.assertEqual(my_func(3, 2), 3)
        self.assertEqual(my_func(3, 2), 3)

    def test_invalidate_nonexisting(self):
        @cached(60)
        def foo(x):
            return 1
        foo.invalidate(5)  # this shouldn't raise exception


class DecoratorTest(ClearMemcachedTest):

    def test_decorator(self):
        self._x = 0

        @cached(60, group='test-group')
        def my_func(params=""):
            self._x = self._x + 1
            return u"%d%s" % (self._x, params)

        self.assertEqual(my_func(), "1")
        self.assertEqual(my_func(), "1")

        self.assertEqual(my_func("x"), u"2x")
        self.assertEqual(my_func("x"), u"2x")

        self.assertEqual(my_func(u"Василий"), u"3Василий")
        self.assertEqual(my_func(u"Василий"), u"3Василий")

        self.assertEqual(my_func(u"й"*240), u"4"+u"й"*240)
        self.assertEqual(my_func(u"й"*240), u"4"+u"й"*240)

        self.assertEqual(my_func(u"Ы"*500), u"5"+u"Ы"*500)
        self.assertEqual(my_func(u"Ы"*500), u"5"+u"Ы"*500)

    def test_key_override(self):
        """
        Test the cache key naming.
        """

        @cached(60*5, fn_key='foo')
        def foo():
            return 'test'

        key = foo.get_cache_key()
        self.assertEqual(key, '[cached]foo(((),{}))')

        # Now test with args and kwargs
        @cached(60*5, fn_key='func_with_args')
        def bar(i, foo='bar'):
            return i * 5

        key = bar.get_cache_key(2, foo='hello')
        self.assertEqual(key, "[cached]func_with_args(((2,),{'foo':'hello'}))")

    def test_legacy_key(self):
        # Now test with args and kwargs
        @cached(60*5, fn_key=legacy_fn_key, key=legacy_key)
        def bar(i, foo='bar'):
            return i * 5

        key = bar.get_cache_key(2, foo='hello')
        self.assertEqual(
            key,
            "[cached]cache_utils.tests.bar:123((2,){'foo':'hello'})"
        )

        self.assertEqual(
            bar.get_cache_key(),
            "[cached]cache_utils.tests.bar:123()"
        )

        self.assertEqual(
            bar.get_cache_key(foo='hello'),
            "[cached]cache_utils.tests.bar:123({'foo':'hello'})"
        )

    def test_key(self):
        """Test that given a custom list of arguments, you use that
        to form the name."""
        def normalize_url(url):
            return url.rstrip('/').replace('https://', 'http://').lower()
        url = 'http://Example.Com'

        @cached(60, key=normalize_url, fn_key='foo_func')
        def foo(url):
            return 'test'

        key = foo.get_cache_key(url)
        self.assertEqual(
            key,
            "[cached]foo_func({})".format(normalize_url(url))
        )

        @cached(60, key=normalize_url, fn_key=lambda fn: fn.__name__ + 'bar')
        def foo(url):
            return 'test'
        key = foo.get_cache_key(url)
        self.assertEqual(
            key,
            "[cached]foobar({})".format(normalize_url(url))
        )

    def test_key_can_return_any_python_value(self):
        @cached(60, key=lambda x, y: x * y, fn_key='foo')
        def foo(a, b):
            return a + b

        key = foo.get_cache_key(2, 3)
        self.assertEqual(key, "[cached]foo(6)")
