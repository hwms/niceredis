# -*- coding: utf-8 *-*
from redis._compat import b

from ..conftest import skip_if_server_version_lt


class TestHyperloglogCommands(object):
    @skip_if_server_version_lt('2.8.9')
    def test_pfadd(self, r):
        members = set([b('1'), b('2'), b('3')])
        assert r.pfadd('a', *members) == 1
        assert r.pfadd('a', *members) == 0
        assert r.pfcount('a') == len(members)

    @skip_if_server_version_lt('2.8.9')
    def test_pfcount(self, r):
        members = set([b('1'), b('2'), b('3')])
        r.pfadd('a', *members)
        assert r.pfcount('a') == len(members)

    @skip_if_server_version_lt('2.8.9')
    def test_pfmerge(self, r):
        mema = set([b('1'), b('2'), b('3')])
        memb = set([b('2'), b('3'), b('4')])
        memc = set([b('5'), b('6'), b('7')])
        r.pfadd('a', *mema)
        r.pfadd('b', *memb)
        r.pfadd('c', *memc)
        r.pfmerge('d', 'c', 'a')
        assert r.pfcount('d') == 6
        r.pfmerge('d', 'b')
        assert r.pfcount('d') == 7
