# -*- coding: utf-8 *-*
import sys
import warnings
from itertools import chain, imap, izip

from redis.exceptions import (ConnectionError, ExecAbortError, RedisError, ResponseError,
                              TimeoutError, WatchError)

from .base import RedisBase


class PipelineCommands(RedisBase):
    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        class Pipeline(BasePipeline, self.__class__):
            "Pipeline for the Redis class"
            pass

        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def watch(self, *names):
        """
        Watches the values at keys ``names``, or None if the key doesn't exist
        """
        warnings.warn(DeprecationWarning('Call WATCH from a Pipeline object'))

    def unwatch(self):
        """
        Unwatches the value at key ``name``, or None of the key doesn't exist
        """
        warnings.warn(
            DeprecationWarning('Call UNWATCH from a Pipeline object'))


class BasePipeline(object):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = set(('DISCARD', 'EXEC', 'UNWATCH'))

    def __init__(self, connection_pool, response_callbacks, transaction,
                 shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def reset(self):
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command('UNWATCH')
                self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise RedisError('Commands without an initial WATCH have already '
                             'been issued')
        self.explicit_transaction = True

    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == 'WATCH') and \
                not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = self.connection_pool.get_connection(command_name,
                                                       self.shard_hint)
            self.connection = conn
        try:
            conn.send_command(*args)
            return self.parse_response(conn, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we're not already watching, we can safely retry the command
            try:
                if not self.watching:
                    conn.send_command(*args)
                    return self.parse_response(conn, command_name, **options)
            except ConnectionError:
                # the retry failed so cleanup.
                conn.disconnect()
                self.reset()
                raise

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    def _execute_transaction(self, connection, commands, raise_on_error):
        cmds = chain([(('MULTI',), {})], commands, [(('EXEC',), {})])
        all_cmds = connection.pack_commands([args for args, _ in cmds])
        connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            self.parse_response(connection, '_')
        except ResponseError:
            errors.append((0, sys.exc_info()[1]))

        # and all the other commands
        for i, command in enumerate(commands):
            try:
                self.parse_response(connection, '_')
            except ResponseError:
                ex = sys.exc_info()[1]
                self.annotate_exception(ex, i + 1, command[0])
                errors.append((i, ex))

        # parse the EXEC.
        try:
            response = self.parse_response(connection, '_')
        except ExecAbortError:
            if self.explicit_transaction:
                self.immediate_execute_command('DISCARD')
            if errors:
                raise errors[0][1]
            raise sys.exc_info()[1]

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in izip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)
        return data

    def _execute_pipeline(self, connection, commands, raise_on_error):
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands([args for args, _ in commands])
        connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(
                    self.parse_response(connection, args[0], **options))
            except ResponseError:
                response.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = unicode(' ').join(imap(unicode, command))
        msg = unicode('Command # %d (%s) of pipeline caused error: %s') % (
            number, cmd, unicode(exception.args[0]))
        exception.args = (msg,) + exception.args[1:]

    def parse_response(self, connection, command_name, **options):
        from .client import StrictRedis
        result = StrictRedis.parse_response(
            self, connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == 'WATCH':
            self.watching = True
        return result

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = immediate('SCRIPT', 'EXISTS', *shas, **{'parse': 'EXISTS'})
        if not all(exists):
            for s, exist in izip(scripts, exists):
                if not exist:
                    s.sha = immediate('SCRIPT', 'LOAD', s.script,
                                      **{'parse': 'LOAD'})

    def execute(self, raise_on_error=True):
        "Execute all the commands in the current pipeline"
        stack = self.command_stack
        if not stack:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('MULTI',
                                                       self.shard_hint)
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return execute(conn, stack, raise_on_error)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError
            if self.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return execute(conn, stack, raise_on_error)
        finally:
            self.reset()

    def watch(self, *names):
        "Watches the values at keys ``names``"
        if self.explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')
        return self.execute_command('WATCH', *names)

    def unwatch(self):
        "Unwatches all previously specified keys"
        return self.watching and self.execute_command('UNWATCH') or True

    def script_load_for_pipeline(self, script):
        "Make sure scripts are loaded prior to pipeline execution"
        # we need the sha now so that Script.__call__ can use it to run
        # evalsha.
        if not script.sha:
            script.sha = self.immediate_execute_command('SCRIPT', 'LOAD',
                                                        script.script,
                                                        **{'parse': 'LOAD'})
        self.scripts.add(script)
