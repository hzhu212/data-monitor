# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 用户自定义 jiaja2 过滤器函数
@CreateAt:    2019-03-31
"""


import datetime
from dateutil import parser as dateparser

from ..context import register_filter


@register_filter
def dt_add(dt, **kwargs):
    """在 datetime 的基础上加上一个偏移量"""
    if not isinstance(dt, datetime.date):
        dt = dateparser.parse(dt)

    # 补丁，使得无论参数加不加 s 都能通过
    patch_keys = ('year', 'month', 'week', 'day', 'hour', 'minute', 'second', 'microsecond')
    params = {}
    for key, value in kwargs.items():
        if key in patch_keys:
            params[key+'s'] = value
        else:
            params[key] = value
    return dt + datetime.timedelta(**params)


@register_filter
def dt_set(dt, **kwargs):
    """重设 datetime 中的某个字段，如 dt_set(dt, day=1, hour=9)"""
    if not isinstance(dt, datetime.date):
        dt = dateparser.parse(dt)

    return dt.replace(**kwargs)


@register_filter
def dt_format(dt, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    """返回一个 datetime 的格式化字符串。
    默认使用 ISO 8601 格式：YYYY-MM-DDTHH:MM:SS[.mmmmmm][+HH:MM]。
    """
    if not isinstance(dt, datetime.date):
        dt = dateparser.parse(dt)

    return dt.strftime(fmt)
