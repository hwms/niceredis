niceredis
=========

Extension for the Python interface to the Redis key-value store
`redis-py <https://github.com/andymccurdy/redis-py>`_


.. image:: https://secure.travis-ci.org/katakumpo/niceredis.png?branch=master
  :target: http://travis-ci.org/katakumpo/niceredis

.. image:: https://coveralls.io/repos/katakumpo/niceredis/badge.png
  :target: https://coveralls.io/r/katakumpo/niceredis

Niceredis provides as features:
-------------------------------
- Unified client class where strict_redis parameter controlls handling of
  some methods instead of two classes, which helps to subclass the client
- serializer implementation, so you can use simplejson for almost all commands
  and do stuff like:

  - redis = JsonRedis()

    key = tuple('A', 'B')

    redis.set(key, 1)

    1 == redis.get(key)


redis-py is developed and maintained by Andy McCurdy (sedrik@gmail.com).
It can be found here: http://github.com/andymccurdy/redis-py
