# -*- coding: utf-8 *-*
from redis.exceptions import NoScriptError

from .base import RedisBase
from .pipeline import BasePipeline


class ScriptCommands(RedisBase):
    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVAL', script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        return self.execute_command('SCRIPT EXISTS', *args)

    def script_flush(self):
        "Flush all scripts from the script cache"
        return self.execute_command('SCRIPT FLUSH')

    def script_kill(self):
        "Kill the currently executing Lua script"
        return self.execute_command('SCRIPT KILL')

    def script_load(self, script):
        "Load a Lua ``script`` into the script cache. Returns the SHA."
        return self.execute_command('SCRIPT LOAD', script)

    def register_script(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)


class Script(object):
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = ''

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, BasePipeline):
            # make sure this script is good to go on pipeline
            client.script_load_for_pipeline(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)
