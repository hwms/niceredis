# -*- coding: utf-8 *-*
import datetime
import time

import pytest
import redis
from redis._compat import b, iteritems, u, unichr

from ..conftest import skip_if_server_version_lt


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float('%s.%s' % (seconds, milliseconds))
    return datetime.datetime.fromtimestamp(timestamp)


class TestKeyCommands(object):
    def test_command_on_invalid_key_type(self, r):
        r.lpush('a', '1')
        with pytest.raises(redis.ResponseError):
            r['a']

    def test_delete(self, r):
        assert r.delete('a') == 0
        r['a'] = 'foo'
        assert r.delete('a') == 1

    def test_delete_with_multiple_keys(self, r):
        r['a'] = 'foo'
        r['b'] = 'bar'
        assert r.delete('a', 'b') == 2
        assert r.get('a') is None
        assert r.get('b') is None

    def test_delitem(self, r):
        r['a'] = 'foo'
        del r['a']
        assert r.get('a') is None

    @skip_if_server_version_lt('2.6.0')
    def test_dump_and_restore(self, r):
        r['a'] = 'foo'
        dumped = r.dump('a')
        del r['a']
        r.restore('a', 0, dumped)
        assert r['a'] == b('foo')

    def test_exists(self, r):
        assert not r.exists('a')
        r['a'] = 'foo'
        assert r.exists('a')

    def test_exists_contains(self, r):
        assert 'a' not in r
        r['a'] = 'foo'
        assert 'a' in r

    def test_expire(self, r):
        assert not r.expire('a', 10)
        r['a'] = 'foo'
        assert r.expire('a', 10)
        assert 0 < r.ttl('a') <= 10
        assert r.persist('a')
        assert not r.ttl('a')

    def test_expireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        assert r.expireat('a', expire_at)
        assert 0 < r.ttl('a') <= 61

    def test_expireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert not r.expireat('a', expire_at)

    def test_expireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        assert r.expireat('a', expire_at_seconds)
        assert 0 < r.ttl('a') <= 61

    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get('a') is None
        byte_string = b('value')
        integer = 5
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        assert r.set('byte_string', byte_string)
        assert r.set('integer', 5)
        assert r.set('unicode_string', unicode_string)
        assert r.get('byte_string') == byte_string
        assert r.get('integer') == b(str(integer))
        assert r.get('unicode_string').decode('utf-8') == unicode_string

    def test_binary_get_set(self, r):
        assert r.set(' foo bar ', '123')
        assert r.get(' foo bar ') == b('123')

        assert r.set(' foo\r\nbar\r\n ', '456')
        assert r.get(' foo\r\nbar\r\n ') == b('456')

        assert r.set(' \r\n\t\x07\x13 ', '789')
        assert r.get(' \r\n\t\x07\x13 ') == b('789')

        assert sorted(r.keys('*')) == \
            [b(' \r\n\t\x07\x13 '), b(' foo\r\nbar\r\n '), b(' foo bar ')]

        assert r.delete(' foo bar ')
        assert r.delete(' foo\r\nbar\r\n ')
        assert r.delete(' \r\n\t\x07\x13 ')

    def test_getitem_and_setitem(self, r):
        r['a'] = 'bar'
        assert r['a'] == b('bar')

    def test_getitem_raises_keyerror_for_missing_key(self, r):
        with pytest.raises(KeyError):
            r['a']

    def test_getset(self, r):
        assert r.getset('a', 'foo') is None
        assert r.getset('a', 'bar') == b('foo')
        assert r.get('a') == b('bar')

    def test_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = set([b('test_a'), b('test_b')])
        keys = keys_with_underscores.union(set([b('testc')]))
        for key in keys:
            r[key] = 1
        assert set(r.keys(pattern='test_*')) == keys_with_underscores
        assert set(r.keys(pattern='test*')) == keys

    def test_mget(self, r):
        assert r.mget(['a', 'b']) == [None, None]
        r['a'] = '1'
        r['b'] = '2'
        r['c'] = '3'
        assert r.mget('a', 'other', 'b', 'c') == [b('1'), None, b('2'), b('3')]

    def test_mset(self, r):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert r.mset(d)
        for k, v in iteritems(d):
            assert r[k] == v

    def test_mset_kwargs(self, r):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert r.mset(**d)
        for k, v in iteritems(d):
            assert r[k] == v

    def test_msetnx(self, r):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert r.msetnx(d)
        d2 = {'a': b('x'), 'd': b('4')}
        assert not r.msetnx(d2)
        for k, v in iteritems(d):
            assert r[k] == v
        assert r.get('d') is None

    def test_msetnx_kwargs(self, r):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert r.msetnx(**d)
        d2 = {'a': b('x'), 'd': b('4')}
        assert not r.msetnx(**d2)
        for k, v in iteritems(d):
            assert r[k] == v
        assert r.get('d') is None

    @skip_if_server_version_lt('2.6.0')
    def test_pexpire(self, r):
        assert not r.pexpire('a', 60000)
        r['a'] = 'foo'
        assert r.pexpire('a', 60000)
        assert 0 < r.pttl('a') <= 60000
        assert r.persist('a')
        assert r.pttl('a') is None

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        assert r.pexpireat('a', expire_at)
        assert 0 < r.pttl('a') <= 61000

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert not r.pexpireat('a', expire_at)

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        assert r.pexpireat('a', expire_at_seconds)
        assert 0 < r.pttl('a') <= 61000

    @skip_if_server_version_lt('2.6.0')
    def test_psetex(self, r):
        assert r.psetex('a', 1000, 'value')
        assert r['a'] == b('value')
        assert 0 < r.pttl('a') <= 1000

    @skip_if_server_version_lt('2.6.0')
    def test_psetex_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.psetex('a', expire_at, 'value')
        assert r['a'] == b('value')
        assert 0 < r.pttl('a') <= 1000

    def test_randomkey(self, r):
        assert r.randomkey() is None
        for key in ('a', 'b', 'c'):
            r[key] = 1
        assert r.randomkey() in (b('a'), b('b'), b('c'))

    def test_rename(self, r):
        r['a'] = '1'
        assert r.rename('a', 'b')
        assert r.get('a') is None
        assert r['b'] == b('1')

    def test_renamenx(self, r):
        r['a'] = '1'
        r['b'] = '2'
        assert not r.renamenx('a', 'b')
        assert r['a'] == b('1')
        assert r['b'] == b('2')

    @skip_if_server_version_lt('2.6.0')
    def test_set_nx(self, r):
        assert r.set('a', '1', nx=True)
        assert not r.set('a', '2', nx=True)
        assert r['a'] == b('1')

    @skip_if_server_version_lt('2.6.0')
    def test_set_xx(self, r):
        assert not r.set('a', '1', xx=True)
        assert r.get('a') is None
        r['a'] = 'bar'
        assert r.set('a', '2', xx=True)
        assert r.get('a') == b('2')

    @skip_if_server_version_lt('2.6.0')
    def test_set_px(self, r):
        assert r.set('a', '1', px=10000)
        assert r['a'] == b('1')
        assert 0 < r.pttl('a') <= 10000
        assert 0 < r.ttl('a') <= 10

    @skip_if_server_version_lt('2.6.0')
    def test_set_px_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.set('a', '1', px=expire_at)
        assert 0 < r.pttl('a') <= 1000
        assert 0 < r.ttl('a') <= 1

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex(self, r):
        assert r.set('a', '1', ex=10)
        assert 0 < r.ttl('a') <= 10

    def test_strict_set_ex(self, sr):
        assert sr.setex('a', 60, '1')
        assert sr['a'] == b('1')
        assert 0 < sr.ttl('a') <= 60

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex_timedelta(self, r):
        expire_at = datetime.timedelta(seconds=60)
        assert r.set('a', '1', ex=expire_at)
        assert 0 < r.ttl('a') <= 60

    @skip_if_server_version_lt('2.6.0')
    def test_set_multipleoptions(self, r):
        r['a'] = 'val'
        assert r.set('a', '1', xx=True, px=10000)
        assert 0 < r.ttl('a') <= 10

    def test_setex(self, r):
        assert r.setex('a', '1', 60)
        assert r['a'] == b('1')
        assert 0 < r.ttl('a') <= 60

    def test_setnx(self, r):
        assert r.setnx('a', '1')
        assert r['a'] == b('1')
        assert not r.setnx('a', '2')
        assert r['a'] == b('1')

    def test_type(self, r):
        assert r.type('a') == b('none')
        r['a'] = '1'
        assert r.type('a') == b('string')
        del r['a']
        r.lpush('a', '1')
        assert r.type('a') == b('list')
        del r['a']
        r.sadd('a', '1')
        assert r.type('a') == b('set')
        del r['a']
        r.zadd('a', **{'1': 1})
        assert r.type('a') == b('zset')

    @skip_if_server_version_lt('2.8.0')
    def test_scan(self, r):
        r.set('a', 1)
        r.set('b', 2)
        r.set('c', 3)
        cursor, keys = r.scan()
        assert cursor == 0
        assert set(keys) == set([b('a'), b('b'), b('c')])
        _, keys = r.scan(match='a')
        assert set(keys) == set([b('a')])

    @skip_if_server_version_lt('2.8.0')
    def test_scan_iter(self, r):
        r.set('a', 1)
        r.set('b', 2)
        r.set('c', 3)
        keys = list(r.scan_iter())
        assert set(keys) == set([b('a'), b('b'), b('c')])
        keys = list(r.scan_iter(match='a'))
        assert set(keys) == set([b('a')])

    def test_strict_ttl(self, sr):
        assert not sr.expire('a', 10)
        sr['a'] = '1'
        assert sr.expire('a', 10)
        assert 0 < sr.ttl('a') <= 10
        assert sr.persist('a')
        assert sr.ttl('a') == -1

    @skip_if_server_version_lt('2.6.0')
    def test_strict_pttl(self, sr):
        assert not sr.pexpire('a', 10000)
        sr['a'] = '1'
        assert sr.pexpire('a', 10000)
        assert 0 < sr.pttl('a') <= 10000
        assert sr.persist('a')
        assert sr.pttl('a') == -1
