# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 用户自定义校验函数
@CreateAt:    2019-03-31
"""

import dateutil
import datetime
from functools import partial
import operator

import pandas as pd

from ..context import register_validator
from ..util import AlarmInfo


@register_validator
def naive_check(result):
    return result > 0


@register_validator
def claim(data, pred=None, serial=True, period='day', start=None, end=None):
    """断言一组数据。
    data 为要断言的数据，可包含多列，程序会假定最后一列为 value，前面所有列为 key。
    pred 是一个谓词函数，即接收一个参数并返回一个布尔值的函数，已定义好的函数包括 gt, ge, lt, le, eq, ne。
    谓词判断失败会触发报警，且报警信息中会给出所有判断失败的行。

    通过上述方式，程序可以监控 data 中所有不满足条件的行，但却无法监控 data 中不存在的行。
    例如某一天的数据缺失，就无法被监控到。因此提供了以下几个参数，用于监控连续序列：
    - serial: 是否要开启连续序列监控，默认开启。
    - period: 连续序列的周期，可以枚举以下几个值：year, month, week, day, hour
    - start: 连续序列的起始时间（包含），datetime 类型。如果不提供，则取 data 中检测到的最小日期。
    - end: 连续序列的结束时间（包含），datetime 类型。如果不提供，则取 data 中检测到的最大日期。

    当监控连续序列开启时，程序会认为 data 的第一列为要监控的序列。
    """

    # 如果 data 是单个值，直接判定
    if not isinstance(data, (tuple, list)):
        ok = pred(data)
        return ok

    if len(data) == 0:
        return False, 'result is empty'

    # 获取列名称序列
    def get_fields(data):
        """尝试获取 data(SQL 查询结果) 的字段列表"""
        try:
            return data[0]._fields
        except AttributeError:
            return ['col' + str(i) for i in range(len(data[0]))]

    col_names = get_fields(data)
    df = pd.DataFrame(data, columns=col_names)

    # 增加一个 flag 列，以便判断哪些行缺数
    df['has_data'] = 'Yes'

    # 如果要检查序列，则生成一个完整序列，并与 data 做 left join
    if serial:
        df = _sequenced(df, col_names[0], period, start, end)

    # 执行数据检查，选出缺数的行以及不满足条件的行
    index = df['has_data'].isna()
    if pred is not None:
        index = index | (~df[col_names[-1]].apply(pred))
    res = df.loc[index, :]

    if len(res.index) == 0:
        return True
    res.reset_index(inplace=True, drop=True)
    res['has_data'].fillna('缺数', inplace=True)
    return False, AlarmInfo('claim', res)


def _sequenced(df, serial_col, period, start, end):
    """填充 DataFrame 的 serial 列，使之连续，其余列会自动填充 NaN。"""

    # 解析 period 参数
    if period not in ('year', 'month', 'week', 'day', 'hour'):
        raise ValueError('argument "period" should be one of (year, month, week, day, hour), but {!r} got'.format(period))

    # 解析 start、end 参数
    if start is not None:
        try:
            start = dateutil.parser.parse(start)
        except:
            raise ValueError('argument "start" ({!r}) can not be parsed as datetime'.format(start))
    if end is not None:
        try:
            end = dateutil.parser.parse(end)
        except:
            raise ValueError('argument "end" ({!r}) can not be parsed as datetime'.format(end))

    # 有些数据库中使用整数、字符串等类型存储日期，需要转化一下
    if not isinstance(df[serial_col][0], datetime.date):
        df[serial_col] = df[serial_col].astype(basestring)
        try:
            df[serial_col] = df[serial_col].apply(dateutil.parser.parse)
        except:
            raise ValueError('the serial column can not be parsed as datetime:\n{}'.format(df[serial_col].head(20)))

    if start is None:
        start = df[serial_col].min()
        # 从数据库直接读取的值可能是 date 类型，需要转化为 datetime 类型
        if not isinstance(start, datetime.datetime):
            start = datetime.datetime.combine(start, datetime.datetime.min.time())
    if end is None:
        end = df[serial_col].max()
        if not isinstance(end, datetime.datetime):
            end = datetime.datetime.combine(end, datetime.datetime.min.time())

    def gen_sequence(start, end, period):
        """根据起止时间和周期，产生完整的日期时间序列"""
        delta = eval('dateutil.relativedelta.relativedelta({}s=1)'.format(period))
        while start <= end:
            yield start
            start = start + delta

    sequence = list(gen_sequence(start, end, period))
    baseline = pd.DataFrame({serial_col: sequence})

    # 根据 period 转化为对应的 format
    format_map = {'year': '%Y', 'month': '%Y-%m', 'day': '%Y-%m-%d', 'hour': '%Y-%m-%d %H'}
    format_map['week'] = format_map['day']
    format_ = format_map[period]
    df[serial_col] = df[serial_col].apply(lambda dt: dt.strftime(format_))
    baseline[serial_col] = baseline[serial_col].apply(lambda dt: dt.strftime(format_))

    df = pd.merge(baseline, df, how='outer', on=serial_col)
    return df


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
