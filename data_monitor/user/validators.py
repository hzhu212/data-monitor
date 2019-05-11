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
def diff(result, threshold=1e-6):
    """diff 两组数据。
    每组数据可包含多列，程序会假定最后一列为 value，前面所有列为 key
    """
    if len(result) != 2:
        raise ValueError('parameter of naive_diff should be a 2-tuple')
    data1, data2 = result

    if len(data1) == 0 and len(data2) == 0:
        return True

    try:
        sample_row = data1[0]
    except IndexError:
        sample_row = data2[0]

    try:
        col_names = sample_row._fields
    except:
        col_names = ['col' + str(i) for i in range(len(row))]

    import pandas as pd

    df1 = pd.DataFrame(data1, columns=col_names)
    df2 = pd.DataFrame(data2, columns=col_names)

    suffixes=('_1', '_2')
    df_all = df1.merge(df2, how='outer', on=col_names[:-1], suffixes=suffixes)

    col1, col2 = [col_names[-1] + sfx for sfx in suffixes]
    try:
        diff = df_all[col1] - df_all[col2]
        index = abs(diff) > threshold | diff.isna()
    except:
        diff = None
        index = df_all[col1] != df_all[col2]

    res = df_all.loc[index, :]
    if len(res.index) == 0:
        return True

    if diff is not None:
        res = res.assign(diff=diff)
    info = ValidateFailInfo('diff', res)
    return False, info
