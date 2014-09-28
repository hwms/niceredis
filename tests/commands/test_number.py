# -*- coding: utf-8 *-*
from redis._compat import b

from ..conftest import skip_if_server_version_lt


class TestNumberCommands(object):
    def test_decr(self, r):
        assert r.decr('a') == -1
        assert r['a'] == b('-1')
        assert r.decr('a') == -2
        assert r['a'] == b('-2')
        assert r.decr('a', amount=5) == -7
        assert r['a'] == b('-7')

    def test_incr(self, r):
        assert r.incr('a') == 1
        assert r['a'] == b('1')
        assert r.incr('a') == 2
        assert r['a'] == b('2')
        assert r.incr('a', amount=5) == 7
        assert r['a'] == b('7')

    def test_incrby(self, r):
        assert r.incrby('a') == 1
        assert r.incrby('a', 4) == 5
        assert r['a'] == b('5')

    @skip_if_server_version_lt('2.6.0')
    def test_incrbyfloat(self, r):
        assert r.incrbyfloat('a') == 1.0
        assert r['a'] == b('1')
        assert r.incrbyfloat('a', 1.1) == 2.1
        assert float(r['a']) == float(2.1)

    def test_floating_point_encoding(self, r):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        r.zadd('a', 'a1', timestamp)
        assert r.zscore('a', 'a1') == timestamp
