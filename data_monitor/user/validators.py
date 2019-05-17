# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 用户自定义校验函数
@CreateAt:    2019-03-31
"""

from functools import partial
import operator

from ..context import register_validator
from ..util import AlarmInfo


@register_validator
def naive_check(result):
    return result > 0


@register_validator
def claim(data, pred):
    """断言一组数据。
    数据可包含多列，程序会假定最后一列为 value，前面所有列为 key。
    pred（谓词）参数必须是一个函数，已定义好的函数包括 gt, ge, lt, le, eq, ne。
    谓词判断失败会触发报警，且报警信息中会给出所有判断失败的行。
    """
    # 如果 data 是单个值，直接判定
    if not isinstance(data, (tuple, list)):
        ok = pred(data)
        return ok

    if len(data) == 0:
        return False, 'result is empty'

    def get_fields(data):
        """尝试获取 data(SQL 查询结果) 的字段列表"""
        try:
            return data[0]._fields
        except AttributeError:
            return ['col' + str(i) for i in range(len(data[0]))]

    col_names = get_fields(data)

    import pandas as pd
    df = pd.DataFrame(data, columns=col_names)
    index = df[col_names[-1]].apply(pred)
    res = df.loc[~index, :]
    if len(res.index) == 0:
        return True

    res.reset_index(inplace=True, drop=True)
    return False, AlarmInfo('claim', res)


@register_validator
def diff(data1, data2, threshold=1e-6, direction=0):
    """diff 两组数据。
    每组数据可包含多列，程序会假定最后一列为 value，前面所有列为 key。
    threshold 为警报阈值：diff 列中任意一值超过 threshold 即触发报警（一边为 NULL 值同样触发报警）
    direction 为 diff 的方向：-1 代表左表减右表，1 代表右表减左表，0 代表两表之差取绝对值。默认为 0。
    """
    if direction not in (-1, 0, 1):
        raise ValueError('invalid argument "direction={!r}", should be one value in [-1, 0, 1]'.format(direction))

    if len(data1) == 0:
        return False, 'data1 (the first table) is empty'
    if len(data2) == 0:
        return False, 'data2 (the second table) is empty'

    def get_fields(data):
        """尝试获取 data(SQL 查询结果) 的字段列表"""
        try:
            return data[0]._fields
        except (IndexError, AttributeError):
            return None

    col_names1 = get_fields(data1)
    col_names2 = get_fields(data2)

    if col_names1 is None and col_names2 is None:
        col_names = ['col' + str(i) for i in range(len(data1[0]))]
        col_names1 = col_names
        col_names2 = col_names
    else:
        if col_names1 is None:
            col_names1 = col_names2
        if col_names2 is None:
            col_names2 = col_names1

    import pandas as pd

    df1 = pd.DataFrame(data1, columns=col_names1)
    df2 = pd.DataFrame(data2, columns=col_names2)

    df_all = df1.merge(
        df2, how='outer', left_on=col_names1[:-1], right_on=col_names2[:-1],
        suffixes=('_1', '_2'))

    col1, col2 = df_all.columns.tolist()[-2:]
    try:
        diff = df_all[col1] - df_all[col2]
        diff = abs(diff) if direction == 0 else diff if direction == -1 else -diff
        index = (diff > threshold) | diff.isna()
    except:
        diff = None
        index = df_all[col1] != df_all[col2]

    res = df_all.loc[index, :]
    if len(res.index) == 0:
        return True

    if diff is not None:
        res = res.assign(diff=diff)
    res.reset_index(inplace=True, drop=True)
    info = AlarmInfo('diff', res)
    return False, info


# 注册一些基本的谓词函数（predicate function），如大于、小于等，以便用户在校验表达式中使用。
@register_validator
def gt(b):
    meth = lambda a, b: operator.gt(a, b)
    return partial(meth, b=b)

@register_validator
def ge(b):
    meth = lambda a, b: operator.ge(a, b)
    return partial(meth, b=b)

@register_validator
def lt(b):
    meth = lambda a, b: operator.lt(a, b)
    return partial(meth, b=b)

@register_validator
def le(b):
    meth = lambda a, b: operator.le(a, b)
    return partial(meth, b=b)

@register_validator
def eq(b):
    meth = lambda a, b: operator.eq(a, b)
    return partial(meth, b=b)

@register_validator
def ne(b):
    meth = lambda a, b: operator.ne(a, b)
    return partial(meth, b=b)

# 支持多谓词组合
@register_validator
def ands(*args):
    """把多个谓词取且，得到一个联合谓词"""
    def combined_fun(x):
        return all(pred(x) for pred in args)
    return combined_fun

@register_validator
def ors(*args):
    """把多个谓词取或，得到一个联合谓词"""
    def combined_fun(x):
        return any(pred(x) for pred in args)
    return combined_fun
