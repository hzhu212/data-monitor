# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: data-monitor 辅助工具包
@CreateAt:    2019-03-31
"""


from collections import namedtuple


AlarmInfo = namedtuple('AlarmInfo', ['type', 'content'])

class ValidatorError(ValueError): pass
