# -*- coding: utf-8 -*-

"""
@Author:      zhuhe212
@Email:       zhuhe212@163.com
@Description: 用户自定义 jiaja2 过滤器函数
@CreateAt:    2019-03-31
"""


import datetime
from dateutil import parser as dateparser, relativedelta

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
    return dt + relativedelta.relativedelta(**params)


@register_filter
def dt_set(dt, **kwargs):
    """重设 datetime 中的某个字段，如 dt_set(dt, day=1, hour=9)。
    除年月日时分秒等常规参数之外，还支持 weekday 参数，使用 1~7 分别代表周一到周日。
    """
    if not isinstance(dt, datetime.date):
        dt = dateparser.parse(dt)

    # 补丁，用于支持 weekday 参数
    if 'weekday' in kwargs:
        for conflict_name in ('year', 'month', 'day'):
            if conflict_name in kwargs:
                raise ValueError('dt_set conflict, can not set "{}" and "weekday" at one time'.format(conflict_name))
        if kwargs['weekday'] not in range(1, 8):
            raise ValueError('argument "weekday" should be an integer between 1~7')
        # datetime 默认的 weekday 范围为 0~6，分别代表周一到周日
        weekday = kwargs.pop('weekday') - 1
        dt += datetime.timedelta(days=weekday - dt.weekday())

    return dt.replace(**kwargs)


@register_filter
def dt_format(dt, fmt='%Y-%m-%d %H:%M:%S'):
    """返回一个 datetime 的格式化字符串。
    默认使用 ISO 8601 格式：YYYY-MM-DD HH:MM:SS
    """
    if not isinstance(dt, datetime.date):
        dt = dateparser.parse(dt)

    return dt.strftime(fmt)
