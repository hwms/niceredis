# -*- coding: utf-8 *-*
from redis._compat import b

from ..conftest import skip_if_server_version_lt


class TestSetCommands(object):
    def test_sadd(self, r):
        members = set([b('1'), b('2'), b('3')])
        r.sadd('a', *members)
        assert r.smembers('a') == members

    def test_scard(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.scard('a') == 3

    def test_sdiff(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.sdiff('a', 'b') == set([b('1'), b('2'), b('3')])
        r.sadd('b', '2', '3')
        assert r.sdiff('a', 'b') == set([b('1')])

    def test_sdiffstore(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.sdiffstore('c', 'a', 'b') == 3
        assert r.smembers('c') == set([b('1'), b('2'), b('3')])
        r.sadd('b', '2', '3')
        assert r.sdiffstore('c', 'a', 'b') == 1
        assert r.smembers('c') == set([b('1')])

    def test_sinter(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.sinter('a', 'b') == set()
        r.sadd('b', '2', '3')
        assert r.sinter('a', 'b') == set([b('2'), b('3')])

    def test_sinterstore(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.sinterstore('c', 'a', 'b') == 0
        assert r.smembers('c') == set()
        r.sadd('b', '2', '3')
        assert r.sinterstore('c', 'a', 'b') == 2
        assert r.smembers('c') == set([b('2'), b('3')])

    def test_sismember(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.sismember('a', '1')
        assert r.sismember('a', '2')
        assert r.sismember('a', '3')
        assert not r.sismember('a', '4')

    def test_smembers(self, r):
        r.sadd('a', '1', '2', '3')
        assert r.smembers('a') == set([b('1'), b('2'), b('3')])

    def test_smove(self, r):
        r.sadd('a', 'a1', 'a2')
        r.sadd('b', 'b1', 'b2')
        assert r.smove('a', 'b', 'a1')
        assert r.smembers('a') == set([b('a2')])
        assert r.smembers('b') == set([b('b1'), b('b2'), b('a1')])

    def test_spop(self, r):
        s = [b('1'), b('2'), b('3')]
        r.sadd('a', *s)
        value = r.spop('a')
        assert value in s
        assert r.smembers('a') == set(s) - set([value])

    def test_srandmember(self, r):
        s = [b('1'), b('2'), b('3')]
        r.sadd('a', *s)
        assert r.srandmember('a') in s

    @skip_if_server_version_lt('2.6.0')
    def test_srandmember_multi_value(self, r):
        s = [b('1'), b('2'), b('3')]
        r.sadd('a', *s)
        randoms = r.srandmember('a', number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    def test_srem(self, r):
        r.sadd('a', '1', '2', '3', '4')
        assert r.srem('a', '5') == 0
        assert r.srem('a', '2', '4') == 2
        assert r.smembers('a') == set([b('1'), b('3')])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan(self, r):
        r.sadd('a', 1, 2, 3)
        cursor, members = r.sscan('a')
        assert cursor == 0
        assert set(members) == set([b('1'), b('2'), b('3')])
        _, members = r.sscan('a', match=b('1'))
        assert set(members) == set([b('1')])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan_iter(self, r):
        r.sadd('a', 1, 2, 3)
        members = list(r.sscan_iter('a'))
        assert set(members) == set([b('1'), b('2'), b('3')])
        members = list(r.sscan_iter('a', match=b('1')))
        assert set(members) == set([b('1')])

    def test_sunion(self, r):
        r.sadd('a', '1', '2')
        r.sadd('b', '2', '3')
        assert r.sunion('a', 'b') == set([b('1'), b('2'), b('3')])

    def test_sunionstore(self, r):
        r.sadd('a', '1', '2')
        r.sadd('b', '2', '3')
        assert r.sunionstore('c', 'a', 'b') == 3
        assert r.smembers('c') == set([b('1'), b('2'), b('3')])
