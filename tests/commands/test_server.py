# -*- coding: utf-8 *-*
import datetime

import pytest
from redis._compat import b, u, unichr

from ..conftest import skip_if_server_version_lt


@pytest.fixture()
def slowlog(request, r):
    current_config = r.config_get()
    old_slower_than_value = current_config['slowlog-log-slower-than']
    old_max_legnth_value = current_config['slowlog-max-len']

    def cleanup():
        r.config_set('slowlog-log-slower-than', old_slower_than_value)
        r.config_set('slowlog-max-len', old_max_legnth_value)
    request.addfinalizer(cleanup)

    r.config_set('slowlog-log-slower-than', 0)
    r.config_set('slowlog-max-len', 128)


class TestServerCommands(object):
    def test_client_list(self, r):
        clients = r.client_list()
        assert isinstance(clients[0], dict)
        assert 'addr' in clients[0]

    @skip_if_server_version_lt('2.6.9')
    def test_client_getname(self, r):
        assert r.client_getname() is None

    @skip_if_server_version_lt('2.6.9')
    def test_client_setname(self, r):
        assert r.client_setname('redis_py_test')
        assert r.client_getname() == 'redis_py_test'

    def test_config_get(self, r):
        data = r.config_get()
        assert 'maxmemory' in data
        assert data['maxmemory'].isdigit()

    def test_config_resetstat(self, r):
        r.ping()
        prior_commands_processed = int(r.info()['total_commands_processed'])
        assert prior_commands_processed >= 1
        r.config_resetstat()
        reset_commands_processed = int(r.info()['total_commands_processed'])
        assert reset_commands_processed < prior_commands_processed

    def test_config_set(self, r):
        data = r.config_get()
        rdbname = data['dbfilename']
        try:
            assert r.config_set('dbfilename', 'redis_py_test.rdb')
            assert r.config_get()['dbfilename'] == 'redis_py_test.rdb'
        finally:
            assert r.config_set('dbfilename', rdbname)

    def test_dbsize(self, r):
        r['a'] = 'foo'
        r['b'] = 'bar'
        assert r.dbsize() == 2

    def test_echo(self, r):
        assert r.echo('foo bar') == b('foo bar')

    def test_info(self, r):
        r['a'] = 'foo'
        r['b'] = 'bar'
        info = r.info()
        assert isinstance(info, dict)
        assert info['db9']['keys'] == 2

    def test_lastsave(self, r):
        assert isinstance(r.lastsave(), datetime.datetime)

    def test_object(self, r):
        r['a'] = 'foo'
        assert isinstance(r.object('refcount', 'a'), int)
        assert isinstance(r.object('idletime', 'a'), int)
        assert r.object('encoding', 'a') == b('raw')
        assert r.object('idletime', 'invalid-key') is None

    def test_ping(self, r):
        assert r.ping()

    def test_slowlog_get(self, r, slowlog):
        assert r.slowlog_reset()
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        r.get(unicode_string)
        slowlog = r.slowlog_get()
        assert isinstance(slowlog, list)
        commands = [log['command'] for log in slowlog]

        get_command = b(' ').join((b('GET'), unicode_string.encode('utf-8')))
        assert get_command in commands
        assert b('SLOWLOG RESET') in commands
        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate order.
        assert commands.index(get_command) < commands.index(b('SLOWLOG RESET'))

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]['start_time'], int)
        assert isinstance(slowlog[0]['duration'], int)

    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        r.get('foo')
        r.get('bar')
        slowlog = r.slowlog_get(1)
        assert isinstance(slowlog, list)
        commands = [log['command'] for log in slowlog]
        assert b('GET foo') not in commands
        assert b('GET bar') in commands

    def test_slowlog_length(self, r, slowlog):
        r.get('foo')
        assert isinstance(r.slowlog_len(), int)

    @skip_if_server_version_lt('2.6.0')
    def test_time(self, r):
        t = r.time()
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)
