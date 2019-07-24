# -*- coding: utf-8 -*-

"""
@Author:      zhuhe212
@Email:       zhuhe212@163.com
@Description: 管理数据库连接，使用连接池提高连接复用
@CreateAt:    2019-03-31
"""


import MySQLdb
from DBUtils.PooledDB import PooledDB


# 对每个不同的数据库分别维护一个连接池
_pools = {}


def _create_pool(db_conf, pool_size=10):
    """create connection pool"""

    return PooledDB(
        creator=MySQLdb, maxconnections=pool_size, blocking=True,
        host=db_conf['host'], port=db_conf['port'],
        user=db_conf['user'], passwd=db_conf['password'],
        db=db_conf['database'], charset=db_conf['charset'],
        )


def get_connection(db_conf):
    """get a connection from the corresponding pool"""

    name = db_conf['_name']
    if name not in _pools:
        _pools[name] = _create_pool(db_conf)
    return _pools[name].connection()
