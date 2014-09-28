# -*- coding: utf-8 *-*
import pytest
from redis import exceptions
from redis._compat import b

import binascii

from ..conftest import skip_if_server_version_lt


class TestByteCommands(object):
    def test_append(self, r):
        assert r.append('a', 'a1') == 2
        assert r['a'] == b('a1')
        assert r.append('a', 'a2') == 4
        assert r['a'] == b('a1a2')

    @skip_if_server_version_lt('2.6.0')
    def test_bitcount(self, r):
        r.setbit('a', 5, True)
        assert r.bitcount('a') == 1
        r.setbit('a', 6, True)
        assert r.bitcount('a') == 2
        r.setbit('a', 5, False)
        assert r.bitcount('a') == 1
        r.setbit('a', 9, True)
        r.setbit('a', 17, True)
        r.setbit('a', 25, True)
        r.setbit('a', 33, True)
        assert r.bitcount('a') == 5
        assert r.bitcount('a', 0, -1) == 5
        assert r.bitcount('a', 2, 3) == 2
        assert r.bitcount('a', 2, -1) == 3
        assert r.bitcount('a', -2, -1) == 2
        assert r.bitcount('a', 1, 1) == 1

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not_empty_string(self, r):
        r['a'] = ''
        r.bitop('not', 'r', 'a')
        assert r.get('r') is None

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not(self, r):
        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r['a'] = test_str
        r.bitop('not', 'r', 'a')
        assert int(binascii.hexlify(r['r']), 16) == correct

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not_in_place(self, r):
        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r['a'] = test_str
        r.bitop('not', 'a', 'a')
        assert int(binascii.hexlify(r['a']), 16) == correct

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_single_string(self, r):
        test_str = b('\x01\x02\xFF')
        r['a'] = test_str
        r.bitop('and', 'res1', 'a')
        r.bitop('or', 'res2', 'a')
        r.bitop('xor', 'res3', 'a')
        assert r['res1'] == test_str
        assert r['res2'] == test_str
        assert r['res3'] == test_str

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_string_operands(self, r):
        r['a'] = b('\x01\x02\xFF\xFF')
        r['b'] = b('\x01\x02\xFF')
        r.bitop('and', 'res1', 'a', 'b')
        r.bitop('or', 'res2', 'a', 'b')
        r.bitop('xor', 'res3', 'a', 'b')
        assert int(binascii.hexlify(r['res1']), 16) == 0x0102FF00
        assert int(binascii.hexlify(r['res2']), 16) == 0x0102FFFF
        assert int(binascii.hexlify(r['res3']), 16) == 0x000000FF

    @skip_if_server_version_lt('2.8.7')
    def test_bitpos(self, r):
        key = 'key:bitpos'
        r.set(key, b('\xff\xf0\x00'))
        assert r.bitpos(key, 0) == 12
        assert r.bitpos(key, 0, 2, -1) == 16
        assert r.bitpos(key, 0, -2, -1) == 12
        r.set(key, b('\x00\xff\xf0'))
        assert r.bitpos(key, 1, 0) == 8
        assert r.bitpos(key, 1, 1) == 8
        r.set(key, b('\x00\x00\x00'))
        assert r.bitpos(key, 1) == -1

    @skip_if_server_version_lt('2.8.7')
    def test_bitpos_wrong_arguments(self, r):
        key = 'key:bitpos:wrong:args'
        r.set(key, b('\xff\xf0\x00'))
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 0, end=1) == 12
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 7) == 12

    def test_get_set_bit(self, r):
        # no value
        assert not r.getbit('a', 5)
        # set bit 5
        assert not r.setbit('a', 5, True)
        assert r.getbit('a', 5)
        # unset bit 4
        assert not r.setbit('a', 4, False)
        assert not r.getbit('a', 4)
        # set bit 4
        assert not r.setbit('a', 4, True)
        assert r.getbit('a', 4)
        # set bit 5 again
        assert r.setbit('a', 5, True)
        assert r.getbit('a', 5)

    def test_getrange(self, r):
        r['a'] = 'foo'
        assert r.getrange('a', 0, 0) == b('f')
        assert r.getrange('a', 0, 2) == b('foo')
        assert r.getrange('a', 3, 4) == b('')

    def test_setrange(self, r):
        assert r.setrange('a', 5, 'foo') == 8
        assert r['a'] == b('\0\0\0\0\0foo')
        r['a'] = 'abcdefghijh'
        assert r.setrange('a', 6, '12345') == 11
        assert r['a'] == b('abcdef12345')

    def test_strlen(self, r):
        r['a'] = 'foo'
        assert r.strlen('a') == 3

    def test_substr(self, r):
        r['a'] = '0123456789'
        assert r.substr('a', 0) == b('0123456789')
        assert r.substr('a', 2) == b('23456789')
        assert r.substr('a', 3, 5) == b('345')
        assert r.substr('a', 3, -2) == b('345678')
