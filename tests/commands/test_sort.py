# -*- coding: utf-8 *-*
import pytest
from redis import exceptions
from redis._compat import b


class TestSortCommand(object):
    def test_sort_basic(self, r):
        r.rpush('a', '3', '2', '1', '4')
        assert r.sort('a') == [b('1'), b('2'), b('3'), b('4')]

    def test_sort_limited(self, r):
        r.rpush('a', '3', '2', '1', '4')
        assert r.sort('a', start=1, num=2) == [b('2'), b('3')]

    def test_sort_by(self, r):
        r['score:1'] = 8
        r['score:2'] = 3
        r['score:3'] = 5
        r.rpush('a', '3', '2', '1')
        assert r.sort('a', by='score:*') == [b('2'), b('3'), b('1')]

    def test_sort_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', get='user:*') == [b('u1'), b('u2'), b('u3')]

    def test_sort_get_multi(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', get=('user:*', '#')) == \
            [b('u1'), b('1'), b('u2'), b('2'), b('u3'), b('3')]

    def test_sort_get_groups_two(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', get=('user:*', '#'), groups=True) == \
            [(b('u1'), b('1')), (b('u2'), b('2')), (b('u3'), b('3'))]

    def test_sort_groups_string_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            r.sort('a', get='user:*', groups=True)

    def test_sort_groups_just_one_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            r.sort('a', get=['user:*'], groups=True)

    def test_sort_groups_no_get(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            r.sort('a', groups=True)

    def test_sort_groups_three_gets(self, r):
        r['user:1'] = 'u1'
        r['user:2'] = 'u2'
        r['user:3'] = 'u3'
        r['door:1'] = 'd1'
        r['door:2'] = 'd2'
        r['door:3'] = 'd3'
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', get=('user:*', 'door:*', '#'), groups=True) == \
            [
                (b('u1'), b('d1'), b('1')),
                (b('u2'), b('d2'), b('2')),
                (b('u3'), b('d3'), b('3'))
            ]

    def test_sort_desc(self, r):
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', desc=True) == [b('3'), b('2'), b('1')]

    def test_sort_alpha(self, r):
        r.rpush('a', 'e', 'c', 'b', 'd', 'a')
        assert r.sort('a', alpha=True) == \
            [b('a'), b('b'), b('c'), b('d'), b('e')]

    def test_sort_store(self, r):
        r.rpush('a', '2', '3', '1')
        assert r.sort('a', store='sorted_values') == 3
        assert r.lrange('sorted_values', 0, -1) == [b('1'), b('2'), b('3')]

    def test_sort_all_options(self, r):
        r['user:1:username'] = 'zeus'
        r['user:2:username'] = 'titan'
        r['user:3:username'] = 'hermes'
        r['user:4:username'] = 'hercules'
        r['user:5:username'] = 'apollo'
        r['user:6:username'] = 'athena'
        r['user:7:username'] = 'hades'
        r['user:8:username'] = 'dionysus'

        r['user:1:favorite_drink'] = 'yuengling'
        r['user:2:favorite_drink'] = 'rum'
        r['user:3:favorite_drink'] = 'vodka'
        r['user:4:favorite_drink'] = 'milk'
        r['user:5:favorite_drink'] = 'pinot noir'
        r['user:6:favorite_drink'] = 'water'
        r['user:7:favorite_drink'] = 'gin'
        r['user:8:favorite_drink'] = 'apple juice'

        r.rpush('gods', '5', '8', '3', '1', '2', '7', '6', '4')
        num = r.sort('gods', start=2, num=4, by='user:*:username',
                     get='user:*:favorite_drink', desc=True, alpha=True,
                     store='sorted')
        assert num == 4
        assert r.lrange('sorted', 0, 10) == \
            [b('vodka'), b('milk'), b('gin'), b('apple juice')]
