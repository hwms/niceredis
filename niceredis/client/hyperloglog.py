# -*- coding: utf-8 *-*
from .base import RedisBase


class HyperloglogCommands(RedisBase):
    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        "Adds the specified elements to the specified HyperLogLog."
        return self.execute_command('PFADD', name, *values)

    def pfcount(self, name):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key.
        """
        return self.execute_command('PFCOUNT', name)

    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        return self.execute_command('PFMERGE', dest, *sources)
