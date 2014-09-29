# -*- coding: utf-8 *-*
import datetime
import time as mod_time

from redis._compat import iteritems
from redis.connection import Token
from redis.exceptions import DataError, RedisError

from .base import RedisBase
from .utils import list_or_args


class KeyCommands(RedisBase):
    # BASIC KEY COMMANDS
    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('DEL', *names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return self.execute_command('DUMP', name)

    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('EXISTS', name)
    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            when = int(mod_time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.execute_command('GET', name)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value:
            return value
        raise KeyError(name)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.execute_command('GETSET', name, value)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        args = list_or_args(keys, args)
        return self.execute_command('MGET', *args)

    def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.execute_command('MSET', *items)

    def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSETNX requires **kwargs or a single '
                                 'dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.execute_command('MOVE', name, db)

    def persist(self, name):
        "Removes an expiration on ``name``"
        return self.execute_command('PERSIST', name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            ms = int(time.microseconds / 1000)
            time = (time.seconds + time.days * 24 * 3600) * 1000 + ms
        return self.execute_command('PEXPIRE', name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(mod_time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command('PEXPIREAT', name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            ms = int(time_ms.microseconds / 1000)
            time_ms = (time_ms.seconds + time_ms.days * 24 * 3600) * 1000 + ms
        return self.execute_command('PSETEX', name, time_ms, value)

    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return self.execute_command('PTTL', name)

    def randomkey(self):
        "Returns the name of a random key"
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.execute_command('RENAMENX', src, dst)

    def restore(self, name, ttl, value):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        return self.execute_command('RESTORE', name, ttl, value)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
        pieces = [name, value]
        if ex:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = ex.seconds + ex.days * 24 * 3600
            pieces.append(ex)
        if px:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                ms = int(px.microseconds / 1000)
                px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
            pieces.append(px)

        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        return self.execute_command('SET', *pieces)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.execute_command('SETEX', name, time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('SETNX', name, value)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.execute_command('TYPE', name)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append(Token('BY'))
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append(Token('LIMIT'))
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, basestring):
                pieces.append(Token('GET'))
                pieces.append(get)
            else:
                for g in get:
                    pieces.append(Token('GET'))
                    pieces.append(g)
        if desc:
            pieces.append(Token('DESC'))
        if alpha:
            pieces.append(Token('ALPHA'))
        if store is not None:
            pieces.append(Token('STORE'))
            pieces.append(store)

        if groups:
            if not get or isinstance(get, basestring) or len(get) < 2:
                raise DataError('when using "groups" the "get" argument '
                                'must be specified and contain at least '
                                'two keys')

        options = {'groups': len(get) if groups else None}
        return self.execute_command('SORT', *pieces, **options)

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [cursor]
        if match is not None:
            pieces.extend([Token('MATCH'), match])
        if count is not None:
            pieces.extend([Token('COUNT'), count])
        return self.execute_command('SCAN', *pieces)

    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item
