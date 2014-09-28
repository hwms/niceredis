# -*- coding: utf-8 *-*
from redis._compat import b, iteritems, iterkeys


class TestListCommands(object):
    def test_binary_lists(self, r):
        mapping = {
            b('foo bar'): [b('1'), b('2'), b('3')],
            b('foo\r\nbar\r\n'): [b('4'), b('5'), b('6')],
            b('foo\tbar\x07'): [b('7'), b('8'), b('9')],
        }
        # fill in lists
        for key, value in iteritems(mapping):
            r.rpush(key, *value)

        # check that KEYS returns all the keys as they are
        assert sorted(r.keys('*')) == sorted(list(iterkeys(mapping)))

        # check that it is possible to get list content by key name
        for key, value in iteritems(mapping):
            assert r.lrange(key, 0, -1) == value

    def test_blpop(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        assert r.blpop(['b', 'a'], timeout=1) == (b('b'), b('3'))
        assert r.blpop(['b', 'a'], timeout=1) == (b('b'), b('4'))
        assert r.blpop(['b', 'a'], timeout=1) == (b('a'), b('1'))
        assert r.blpop(['b', 'a'], timeout=1) == (b('a'), b('2'))
        assert r.blpop(['b', 'a'], timeout=1) is None
        r.rpush('c', '1')
        assert r.blpop('c', timeout=1) == (b('c'), b('1'))

    def test_brpop(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        assert r.brpop(['b', 'a'], timeout=1) == (b('b'), b('4'))
        assert r.brpop(['b', 'a'], timeout=1) == (b('b'), b('3'))
        assert r.brpop(['b', 'a'], timeout=1) == (b('a'), b('2'))
        assert r.brpop(['b', 'a'], timeout=1) == (b('a'), b('1'))
        assert r.brpop(['b', 'a'], timeout=1) is None
        r.rpush('c', '1')
        assert r.brpop('c', timeout=1) == (b('c'), b('1'))

    def test_brpoplpush(self, r):
        r.rpush('a', '1', '2')
        r.rpush('b', '3', '4')
        assert r.brpoplpush('a', 'b') == b('2')
        assert r.brpoplpush('a', 'b') == b('1')
        assert r.brpoplpush('a', 'b', timeout=1) is None
        assert r.lrange('a', 0, -1) == []
        assert r.lrange('b', 0, -1) == [b('1'), b('2'), b('3'), b('4')]

    def test_brpoplpush_empty_string(self, r):
        r.rpush('a', '')
        assert r.brpoplpush('a', 'b') == b('')

    def test_lindex(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.lindex('a', '0') == b('1')
        assert r.lindex('a', '1') == b('2')
        assert r.lindex('a', '2') == b('3')

    def test_linsert(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.linsert('a', 'after', '2', '2.5') == 4
        assert r.lrange('a', 0, -1) == [b('1'), b('2'), b('2.5'), b('3')]
        assert r.linsert('a', 'before', '2', '1.5') == 5
        assert r.lrange('a', 0, -1) == \
            [b('1'), b('1.5'), b('2'), b('2.5'), b('3')]

    def test_llen(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.llen('a') == 3

    def test_lpop(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.lpop('a') == b('1')
        assert r.lpop('a') == b('2')
        assert r.lpop('a') == b('3')
        assert r.lpop('a') is None

    def test_lpush(self, r):
        assert r.lpush('a', '1') == 1
        assert r.lpush('a', '2') == 2
        assert r.lpush('a', '3', '4') == 4
        assert r.lrange('a', 0, -1) == [b('4'), b('3'), b('2'), b('1')]

    def test_lpushx(self, r):
        assert r.lpushx('a', '1') == 0
        assert r.lrange('a', 0, -1) == []
        r.rpush('a', '1', '2', '3')
        assert r.lpushx('a', '4') == 4
        assert r.lrange('a', 0, -1) == [b('4'), b('1'), b('2'), b('3')]

    def test_lrange(self, r):
        r.rpush('a', '1', '2', '3', '4', '5')
        assert r.lrange('a', 0, 2) == [b('1'), b('2'), b('3')]
        assert r.lrange('a', 2, 10) == [b('3'), b('4'), b('5')]
        assert r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4'), b('5')]

    def test_lrem(self, r):
        r.rpush('a', '1', '1', '1', '1')
        assert r.lrem('a', '1', 1) == 1
        assert r.lrange('a', 0, -1) == [b('1'), b('1'), b('1')]
        assert r.lrem('a', '1') == 3
        assert r.lrange('a', 0, -1) == []

    def test_strict_lrem(self, sr):
        sr.rpush('a', 'a1', 'a2', 'a3', 'a1')
        sr.lrem('a', 0, 'a1')
        assert sr.lrange('a', 0, -1) == [b('a2'), b('a3')]

    def test_lset(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.lrange('a', 0, -1) == [b('1'), b('2'), b('3')]
        assert r.lset('a', 1, '4')
        assert r.lrange('a', 0, 2) == [b('1'), b('4'), b('3')]

    def test_ltrim(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.ltrim('a', 0, 1)
        assert r.lrange('a', 0, -1) == [b('1'), b('2')]

    def test_rpop(self, r):
        r.rpush('a', '1', '2', '3')
        assert r.rpop('a') == b('3')
        assert r.rpop('a') == b('2')
        assert r.rpop('a') == b('1')
        assert r.rpop('a') is None

    def test_rpoplpush(self, r):
        r.rpush('a', 'a1', 'a2', 'a3')
        r.rpush('b', 'b1', 'b2', 'b3')
        assert r.rpoplpush('a', 'b') == b('a3')
        assert r.lrange('a', 0, -1) == [b('a1'), b('a2')]
        assert r.lrange('b', 0, -1) == [b('a3'), b('b1'), b('b2'), b('b3')]

    def test_rpush(self, r):
        assert r.rpush('a', '1') == 1
        assert r.rpush('a', '2') == 2
        assert r.rpush('a', '3', '4') == 4
        assert r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]

    def test_rpushx(self, r):
        assert r.rpushx('a', 'b') == 0
        assert r.lrange('a', 0, -1) == []
        r.rpush('a', '1', '2', '3')
        assert r.rpushx('a', '4') == 4
        assert r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]
