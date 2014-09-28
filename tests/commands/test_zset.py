# -*- coding: utf-8 *-*
from redis._compat import b

from ..conftest import skip_if_server_version_lt


class TestZsetCommands(object):
    def test_zadd(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zrange('a', 0, -1) == [b('a1'), b('a2'), b('a3')]

    def test_strict_zadd(self, sr):
        sr.zadd('a', 1.0, 'a1', 2.0, 'a2', a3=3.0)
        assert sr.zrange('a', 0, -1, withscores=True) == \
            [(b('a1'), 1.0), (b('a2'), 2.0), (b('a3'), 3.0)]

    def test_zcard(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zcard('a') == 3

    def test_zcount(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zcount('a', '-inf', '+inf') == 3
        assert r.zcount('a', 1, 2) == 2
        assert r.zcount('a', 10, 20) == 0

    def test_zincrby(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zincrby('a', 'a2') == 3.0
        assert r.zincrby('a', 'a3', amount=5) == 8.0
        assert r.zscore('a', 'a2') == 3.0
        assert r.zscore('a', 'a3') == 8.0

    @skip_if_server_version_lt('2.8.9')
    def test_zlexcount(self, r):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert r.zlexcount('a', '-', '+') == 7
        assert r.zlexcount('a', '[b', '[f') == 5

    def test_zinterstore_sum(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zinterstore('d', ['a', 'b', 'c']) == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 8), (b('a1'), 9)]

    def test_zinterstore_max(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zinterstore('d', ['a', 'b', 'c'], aggregate='MAX') == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 5), (b('a1'), 6)]

    def test_zinterstore_min(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        r.zadd('b', a1=2, a2=3, a3=5)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zinterstore('d', ['a', 'b', 'c'], aggregate='MIN') == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a1'), 1), (b('a3'), 3)]

    def test_zinterstore_with_weight(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zinterstore('d', {'a': 1, 'b': 2, 'c': 3}) == 2
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 20), (b('a1'), 23)]

    def test_zrange(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zrange('a', 0, 1) == [b('a1'), b('a2')]
        assert r.zrange('a', 1, 2) == [b('a2'), b('a3')]

        # withscores
        assert r.zrange('a', 0, 1, withscores=True) == \
            [(b('a1'), 1.0), (b('a2'), 2.0)]
        assert r.zrange('a', 1, 2, withscores=True) == \
            [(b('a2'), 2.0), (b('a3'), 3.0)]

        # custom score function
        assert r.zrange('a', 0, 1, withscores=True, score_cast_func=int) == \
            [(b('a1'), 1), (b('a2'), 2)]

    @skip_if_server_version_lt('2.8.9')
    def test_zrangebylex(self, r):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert r.zrangebylex('a', '-', '[c') == [b('a'), b('b'), b('c')]
        assert r.zrangebylex('a', '-', '(c') == [b('a'), b('b')]
        assert r.zrangebylex('a', '[aaa', '(g') == \
            [b('b'), b('c'), b('d'), b('e'), b('f')]
        assert r.zrangebylex('a', '[f', '+') == [b('f'), b('g')]
        assert r.zrangebylex('a', '-', '+', start=3, num=2) == [b('d'), b('e')]

    def test_zrangebyscore(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zrangebyscore('a', 2, 4) == [b('a2'), b('a3'), b('a4')]

        # slicing with start/num
        assert r.zrangebyscore('a', 2, 4, start=1, num=2) == \
            [b('a3'), b('a4')]

        # withscores
        assert r.zrangebyscore('a', 2, 4, withscores=True) == \
            [(b('a2'), 2.0), (b('a3'), 3.0), (b('a4'), 4.0)]

        # custom score function
        assert r.zrangebyscore('a', 2, 4, withscores=True,
                               score_cast_func=int) == \
            [(b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]

    def test_zrank(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zrank('a', 'a1') == 0
        assert r.zrank('a', 'a2') == 1
        assert r.zrank('a', 'a6') is None

    def test_zrem(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zrem('a', 'a2') == 1
        assert r.zrange('a', 0, -1) == [b('a1'), b('a3')]
        assert r.zrem('a', 'b') == 0
        assert r.zrange('a', 0, -1) == [b('a1'), b('a3')]

    def test_zrem_multiple_keys(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zrem('a', 'a1', 'a2') == 2
        assert r.zrange('a', 0, 5) == [b('a3')]

    @skip_if_server_version_lt('2.8.9')
    def test_zremrangebylex(self, r):
        r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert r.zremrangebylex('a', '-', '[c') == 3
        assert r.zrange('a', 0, -1) == [b('d'), b('e'), b('f'), b('g')]
        assert r.zremrangebylex('a', '[f', '+') == 2
        assert r.zrange('a', 0, -1) == [b('d'), b('e')]
        assert r.zremrangebylex('a', '[h', '+') == 0
        assert r.zrange('a', 0, -1) == [b('d'), b('e')]

    def test_zremrangebyrank(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zremrangebyrank('a', 1, 3) == 3
        assert r.zrange('a', 0, 5) == [b('a1'), b('a5')]

    def test_zremrangebyscore(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zremrangebyscore('a', 2, 4) == 3
        assert r.zrange('a', 0, -1) == [b('a1'), b('a5')]
        assert r.zremrangebyscore('a', 2, 4) == 0
        assert r.zrange('a', 0, -1) == [b('a1'), b('a5')]

    def test_zrevrange(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zrevrange('a', 0, 1) == [b('a3'), b('a2')]
        assert r.zrevrange('a', 1, 2) == [b('a2'), b('a1')]

        # withscores
        assert r.zrevrange('a', 0, 1, withscores=True) == \
            [(b('a3'), 3.0), (b('a2'), 2.0)]
        assert r.zrevrange('a', 1, 2, withscores=True) == \
            [(b('a2'), 2.0), (b('a1'), 1.0)]

        # custom score function
        assert r.zrevrange('a', 0, 1, withscores=True,
                           score_cast_func=int) == \
            [(b('a3'), 3.0), (b('a2'), 2.0)]

    def test_zrevrangebyscore(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zrevrangebyscore('a', 4, 2) == [b('a4'), b('a3'), b('a2')]

        # slicing with start/num
        assert r.zrevrangebyscore('a', 4, 2, start=1, num=2) == \
            [b('a3'), b('a2')]

        # withscores
        assert r.zrevrangebyscore('a', 4, 2, withscores=True) == \
            [(b('a4'), 4.0), (b('a3'), 3.0), (b('a2'), 2.0)]

        # custom score function
        assert r.zrevrangebyscore('a', 4, 2, withscores=True,
                                  score_cast_func=int) == \
            [(b('a4'), 4), (b('a3'), 3), (b('a2'), 2)]

    def test_zrevrank(self, r):
        r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert r.zrevrank('a', 'a1') == 4
        assert r.zrevrank('a', 'a2') == 3
        assert r.zrevrank('a', 'a6') is None

    @skip_if_server_version_lt('2.8.0')
    def test_zscan(self, r):
        r.zadd('a', 'a', 1, 'b', 2, 'c', 3)
        cursor, pairs = r.zscan('a')
        assert cursor == 0
        assert set(pairs) == set([(b('a'), 1), (b('b'), 2), (b('c'), 3)])
        _, pairs = r.zscan('a', match='a')
        assert set(pairs) == set([(b('a'), 1)])

    @skip_if_server_version_lt('2.8.0')
    def test_zscan_iter(self, r):
        r.zadd('a', 'a', 1, 'b', 2, 'c', 3)
        pairs = list(r.zscan_iter('a'))
        assert set(pairs) == set([(b('a'), 1), (b('b'), 2), (b('c'), 3)])
        pairs = list(r.zscan_iter('a', match='a'))
        assert set(pairs) == set([(b('a'), 1)])

    def test_zscore(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        assert r.zscore('a', 'a1') == 1.0
        assert r.zscore('a', 'a2') == 2.0
        assert r.zscore('a', 'a4') is None

    def test_zunionstore_sum(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zunionstore('d', ['a', 'b', 'c']) == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 3), (b('a4'), 4), (b('a3'), 8), (b('a1'), 9)]

    def test_zunionstore_max(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zunionstore('d', ['a', 'b', 'c'], aggregate='MAX') == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 2), (b('a4'), 4), (b('a3'), 5), (b('a1'), 6)]

    def test_zunionstore_min(self, r):
        r.zadd('a', a1=1, a2=2, a3=3)
        r.zadd('b', a1=2, a2=2, a3=4)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zunionstore('d', ['a', 'b', 'c'], aggregate='MIN') == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a1'), 1), (b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]

    def test_zunionstore_with_weight(self, r):
        r.zadd('a', a1=1, a2=1, a3=1)
        r.zadd('b', a1=2, a2=2, a3=2)
        r.zadd('c', a1=6, a3=5, a4=4)
        assert r.zunionstore('d', {'a': 1, 'b': 2, 'c': 3}) == 4
        assert r.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 5), (b('a4'), 12), (b('a3'), 20), (b('a1'), 23)]
