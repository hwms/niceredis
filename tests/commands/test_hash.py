# -*- coding: utf-8 *-*

from redis._compat import b, iterkeys, itervalues

from ..conftest import skip_if_server_version_lt


class TestHashCommands(object):
    def test_hget_and_hset(self, r):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert r.hget('a', '1') == b('1')
        assert r.hget('a', '2') == b('2')
        assert r.hget('a', '3') == b('3')

        # field was updated, redis returns 0
        assert r.hset('a', '2', 5) == 0
        assert r.hget('a', '2') == b('5')

        # field is new, redis returns 1
        assert r.hset('a', '4', 4) == 1
        assert r.hget('a', '4') == b('4')

        # key inside of hash that doesn't exist returns null value
        assert r.hget('a', 'b') is None

    def test_hdel(self, r):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert r.hdel('a', '2') == 1
        assert r.hget('a', '2') is None
        assert r.hdel('a', '1', '3') == 2
        assert r.hlen('a') == 0

    def test_hexists(self, r):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert r.hexists('a', '1')
        assert not r.hexists('a', '4')

    def test_hgetall(self, r):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        r.hmset('a', h)
        assert r.hgetall('a') == h

    def test_hincrby(self, r):
        assert r.hincrby('a', '1') == 1
        assert r.hincrby('a', '1', amount=2) == 3
        assert r.hincrby('a', '1', amount=-2) == 1

    @skip_if_server_version_lt('2.6.0')
    def test_hincrbyfloat(self, r):
        assert r.hincrbyfloat('a', '1') == 1.0
        assert r.hincrbyfloat('a', '1') == 2.0
        assert r.hincrbyfloat('a', '1', 1.2) == 3.2

    def test_hkeys(self, r):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        r.hmset('a', h)
        local_keys = list(iterkeys(h))
        remote_keys = r.hkeys('a')
        assert (sorted(local_keys) == sorted(remote_keys))

    def test_hlen(self, r):
        r.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert r.hlen('a') == 3

    def test_hmget(self, r):
        assert r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        assert r.hmget('a', 'a', 'b', 'c') == [b('1'), b('2'), b('3')]

    def test_hmset(self, r):
        h = {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        assert r.hmset('a', h)
        assert r.hgetall('a') == h

    def test_hsetnx(self, r):
        # Initially set the hash field
        assert r.hsetnx('a', '1', 1)
        assert r.hget('a', '1') == b('1')
        assert not r.hsetnx('a', '1', 2)
        assert r.hget('a', '1') == b('1')

    @skip_if_server_version_lt('2.8.0')
    def test_hscan(self, r):
        r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        cursor, dic = r.hscan('a')
        assert cursor == 0
        assert dic == {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        _, dic = r.hscan('a', match='a')
        assert dic == {b('a'): b('1')}

    @skip_if_server_version_lt('2.8.0')
    def test_hscan_iter(self, r):
        r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        dic = dict(r.hscan_iter('a'))
        assert dic == {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        dic = dict(r.hscan_iter('a', match='a'))
        assert dic == {b('a'): b('1')}

    def test_hvals(self, r):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        r.hmset('a', h)
        local_vals = list(itervalues(h))
        remote_vals = r.hvals('a')
        assert sorted(local_vals) == sorted(remote_vals)
