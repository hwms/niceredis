# -*- coding: utf-8 *-*
from redis._compat import imap, nativestr

from ..callbacks import (bool_ok, float_or_none, int_or_none, pairs_to_dict, parse_client_list,
                         parse_config_get, parse_debug_object, parse_hscan, parse_info,
                         parse_object, parse_scan, parse_sentinel_get_master, parse_sentinel_master,
                         parse_sentinel_masters, parse_sentinel_slaves_and_sentinels,
                         parse_slowlog_get, parse_zscan, sort_return_tuples, timestamp_to_datetime,
                         zset_score_pairs)
from .byte import ByteCommands
from .hash import HashCommands
from .hyperloglog import HyperloglogCommands
from .key import KeyCommands
from .list import ListCommands
from .number import NumberCommands
from .pipeline import PipelineCommands
from .pubsub import PubSubCommands
from .script import ScriptCommands
from .server import ServerCommands
from .set import SetCommands
from .utils import dict_merge, string_keys_to_dict
from .zset import ZsetCommands


class StrictRedis(ByteCommands, HashCommands, HyperloglogCommands, KeyCommands, ListCommands,
                   NumberCommands, PipelineCommands, PubSubCommands, ScriptCommands, ServerCommands,
                   SetCommands, ZsetCommands):
    strict_redis = True
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'AUTH EXISTS EXPIRE EXPIREAT HEXISTS HMSET MOVE MSETNX PERSIST '
            'PSETEX RENAMENX SISMEMBER SMOVE SETEX SETNX',
            bool
        ),
        string_keys_to_dict(
            'BITCOUNT BITPOS DECRBY DEL GETBIT HDEL HLEN INCRBY LINSERT LLEN '
            'LPUSHX PFADD PFCOUNT RPUSHX SADD SCARD SDIFFSTORE SETBIT '
            'SETRANGE SINTERSTORE SREM STRLEN SUNIONSTORE ZADD ZCARD '
            'ZLEXCOUNT ZREM ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE',
            int
        ),
        string_keys_to_dict('INCRBYFLOAT HINCRBYFLOAT', float),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: isinstance(r, long) and r or nativestr(r) == 'OK'
        ),
        string_keys_to_dict('SORT', sort_return_tuples),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET PFMERGE RENAME '
            'SAVE SELECT SHUTDOWN SLAVEOF WATCH UNWATCH',
            bool_ok
        ),
        string_keys_to_dict('BLPOP BRPOP', lambda r: r and tuple(r) or None),
        string_keys_to_dict(
            'SDIFF SINTER SMEMBERS SUNION',
            lambda r: r and set(r) or set()
        ),
        string_keys_to_dict(
            'ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE',
            zset_score_pairs
        ),
        string_keys_to_dict('ZRANK ZREVRANK', int_or_none),
        string_keys_to_dict('BGREWRITEAOF BGSAVE', lambda r: True),
        {
            'CLIENT GETNAME': lambda r: r and nativestr(r),
            'CLIENT KILL': bool_ok,
            'CLIENT LIST': parse_client_list,
            'CLIENT SETNAME': bool_ok,
            'CONFIG GET': parse_config_get,
            'CONFIG RESETSTAT': bool_ok,
            'CONFIG SET': bool_ok,
            'DEBUG OBJECT': parse_debug_object,
            'HGETALL': lambda r: r and pairs_to_dict(r) or {},
            'HSCAN': parse_hscan,
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'OBJECT': parse_object,
            'PING': lambda r: nativestr(r) == 'PONG',
            'RANDOMKEY': lambda r: r and r or None,
            'SCAN': parse_scan,
            'SCRIPT EXISTS': lambda r: list(imap(bool, r)),
            'SCRIPT FLUSH': bool_ok,
            'SCRIPT KILL': bool_ok,
            'SCRIPT LOAD': nativestr,
            'SENTINEL GET-MASTER-ADDR-BY-NAME': parse_sentinel_get_master,
            'SENTINEL MASTER': parse_sentinel_master,
            'SENTINEL MASTERS': parse_sentinel_masters,
            'SENTINEL MONITOR': bool_ok,
            'SENTINEL REMOVE': bool_ok,
            'SENTINEL SENTINELS': parse_sentinel_slaves_and_sentinels,
            'SENTINEL SET': bool_ok,
            'SENTINEL SLAVES': parse_sentinel_slaves_and_sentinels,
            'SET': lambda r: r and nativestr(r) == 'OK',
            'SLOWLOG GET': parse_slowlog_get,
            'SLOWLOG LEN': int,
            'SLOWLOG RESET': bool_ok,
            'SSCAN': parse_scan,
            'TIME': lambda x: (int(x[0]), int(x[1])),
            'ZSCAN': parse_zscan
        }
    )


class Redis(StrictRedis):
    """
    Provides backwards compatibility with older versions of redis-py that
    changed arguments to some commands to be more Pythonic, sane, or by
    accident.
    """
    strict_redis = False



