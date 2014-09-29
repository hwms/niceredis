# -*- coding: utf-8 *-*
from redis.exceptions import RedisError

from .base import RedisBase


class ByteCommands(RedisBase):
    # direct byte(string) manipulation commands
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command('APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or \
                (end is not None and start is None):
            raise RedisError("Both start and end must be specified")
        return self.execute_command('BITCOUNT', *params)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return self.execute_command('BITOP', operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` difines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        if bit not in (0, 1):
            raise RedisError('bit must be 0 or 1')
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise RedisError("start argument is not set, "
                             "when end is specified")
        return self.execute_command('BITPOS', *params)

    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return self.execute_command('GETBIT', name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.execute_command('GETRANGE', key, start, end)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command('SETBIT', name, offset, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return self.execute_command('SETRANGE', name, offset, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.execute_command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)
