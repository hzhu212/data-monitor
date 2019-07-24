# -*- coding: utf-8 -*-

"""
@Author:      zhuhe212
@Email:       zhuhe212@163.com
@Description: 处理 jinja2 渲染器上下文，以及用户校验表达式上下文
@CreateAt:    2019-04-01
"""


# 装饰器，支持用户自定义 jinja2 过滤器
# ------------------------------------------------------------------------------
_filters = []

def register_filter(func):
    """注册自定义 jinja2 过滤器的装饰器"""
    _filters.append(func)
    return func


# 装饰器，支持用户自定义校验函数
# ------------------------------------------------------------------------------
_validators = []

def register_validator(func):
    """注册自定义校验函数的装饰器"""
    _validators.append(func)
    return func


# 获取上下文对象
# ------------------------------------------------------------------------------
def get_filter_context():
    """获取包含用户自定义过滤器的上下文环境"""

    # 导入自定义过滤器
    import user.filters

    global _filters
    filter_context = {func.__name__: func for func in _filters}

    return filter_context


def get_validator_context():
    """获取包含用户自定义校验函数的上下文环境"""

    # 导入用户自定义校验函数
    import user.validators

    # 除了自定义过滤器外，还暴露一些内置函数给校验表达式
    _allowed_names_in_validator = (
        'None, False, True, Ellipsis, abs, all, apply, basestring, bin, bool, '
        'bytearray, bytes, chr, cmp, complex, dict, divmod, enumerate, filter, '
        'float, format, frozenset, hash, hex, int, isinstance, issubclass, '
        'len, list, long, map, max, memoryview, min, next, oct, ord, pow, range, '
        'reduce, repr, reversed, round, set, slice, sorted, str, sum, tuple, zip, '
    ).strip(', ').split(', ')

    global _validators

    validator_context = {name: eval(name) for name in _allowed_names_in_validator}
    validator_context.update({func.__name__: func for func in _validators})

    return validator_context
