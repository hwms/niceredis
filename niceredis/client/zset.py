# -*- coding: utf-8 *-*
from redis._compat import iteritems, iterkeys, itervalues
from redis.connection import Token
from redis.exceptions import RedisError

from .base import RedisBase


class ZsetCommands(RedisBase):
    # SORTED SET COMMANDS
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(args)
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command('ZADD', name, *pieces)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.execute_command('ZCOUNT', name, min, max)

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.execute_command('ZLEXCOUNT', name, min, max)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append(Token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYLEX', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token('LIMIT'), start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"
        return self.execute_command('ZREM', name, *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYLEX', name, min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.execute_command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append(Token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend([Token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def _zaggregate(self, command, dest, keys, aggregate=None):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            keys, weights = iterkeys(keys), itervalues(keys)
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(Token('WEIGHTS'))
            pieces.extend(weights)
        if aggregate:
            pieces.append(Token('AGGREGATE'))
            pieces.append(aggregate)
        return self.execute_command(*pieces)

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token('MATCH'), match])
        if count is not None:
            pieces.extend([Token('COUNT'), count])
        options = {'score_cast_func': score_cast_func}
        return self.execute_command('ZSCAN', *pieces, **options)

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item
