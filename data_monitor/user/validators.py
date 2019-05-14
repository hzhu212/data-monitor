# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 用户自定义校验函数
@CreateAt:    2019-03-31
"""


from ..context import register_validator
from ..util import ValidateFailInfo


@register_validator
def naive_check(result):
    return result > 0


@register_validator
def diff(result, threshold=1e-6, direction=0):
    """diff 两组数据。
    每组数据可包含多列，程序会假定最后一列为 value，前面所有列为 key。
    threshold 为警报阈值：diff 列中任意一值超过 threshold 即触发报警（一边为 NULL 值同样触发报警）
    direction 为 diff 的方向：-1 代表左表减右表，1 代表右表减左表，0 代表两表之差取绝对值。默认为 0。
    """
    if direction not in (-1, 0, 1):
        raise ValueError('invalid argument "direction={!r}", should be one value in [-1, 0, 1]'.format(direction))
    if len(result) != 2:
        raise ValueError('parameter of function `diff` should be a 2-tuple')
    data1, data2 = result

    if len(data1) == 0:
        return False, 'result[0] (the first table) is empty'
    if len(data2) == 0:
        return False, 'result[1] (the second table) is empty'

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
    info = ValidateFailInfo('diff', res)
    return False, info
