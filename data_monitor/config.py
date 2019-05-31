# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 处理用户配置相关逻辑，该模块构建了一个配置层，使得配置文件的具体格式与主程序
              逻辑隔离。以防将来需要切换配置格式，比如切换成 JSON，只需要再实现对应的
              配置层（主要是 get_config_list 函数）即可，主程序逻辑无需更改。
@CreateAt:    2019-03-31
"""


import datetime
from dateutil import parser as dateparser
import logging
import os
import re
import traceback
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import jinja2

from .alarm import format_baidu_hi, send_baidu_hi, format_email, send_email
from .context import get_filter_context
from .util import AlarmInfo


logger = logging.getLogger(__name__)
re_time = re.compile(r'^\d{1,2}:\d{1,2}(:\d{1,2})?$')


class ConfigError(Exception): pass


# 通用工具函数
# ------------------------------------------------------------------------------
def get_config(config_file, section_name=None):
    """load configuration file (.cfg/.ini).
    See "https://docs.python.org/2/library/configparser.html" for detail.
    if section_name is None, return the whole config file as ConfigParser object;
    if section_name is given, return the section config as a dict.
    """
    # 默认情况下，配置文件中的 key 是不分大小写的（为了兼容 Windows），此处设为区分大小写
    conf = configparser.ConfigParser()
    conf.optionxform = str

    conf.read(config_file, encoding='utf8')
    if section_name is None:
        return {name: dict(conf.items(name)) for name in conf.sections()}
    return dict(conf.items(section_name))


def detect_configs_conflict(config_files, key=None):
    """检测多个配置文件中发生名称冲突的 section。
    如果有冲突，返回找到的第一个冲突项，以及发生冲突的两个文件。
    key 可接收一个函数，用于筛选配置文件中的 section。默认跳过 DEFAULT section。
    """
    if key is None:
        key = lambda name: name != 'DEFAULT'

    configs = (get_config(file) for file in config_files)
    sections_list = [set(filter(key, config.keys())) for config in configs]
    for i in range(len(config_files) - 1):
        for j in range(i + 1, len(config_files)):
            conflicts = sections_list[i] & sections_list[j]
            if conflicts:
                return conflicts.pop(), config_files[i], config_files[j]


# 检查作业配置
# ------------------------------------------------------------------------------
def check_out_job_config(job_conf, db_configs):
    """检查某个具体作业的配置。如果配置有误，将打印报错信息并跳过该作业。"""
    try:
        return _check_out_job_config(job_conf, db_configs)
    except ConfigError as e:
        logger.error('job [{}] config error: {}'.format(job_conf['_name'], e))
        info = AlarmInfo('config_error', e)
        send_baidu_hi(job_conf['alarm_hi'], format_baidu_hi(job_conf, info))
        send_email(job_conf['alarm_email'], format_email(job_conf, info))
        return None


def _check_out_job_config(job_conf, db_configs):
    """检查用户配置"""

    # 首先将 alarm_hi、alarm_email 准备就绪，以便发生配置错误时可以报警
    job_conf['alarm_hi'] = [s for s in job_conf['alarm_hi'].split(',')] if 'alarm_hi' in job_conf else []
    job_conf['alarm_email'] = [s for s in job_conf['alarm_email'].split(',')] if 'alarm_email' in job_conf else []

    # 检查必选配置项
    REQUIRED_OPTIONS = (
        'desc', 'period', 'is_active', 'alarm_hi', 'alarm_email', 'due_time',
        'db_conf', 'sql', 'validator', )
    for op in REQUIRED_OPTIONS:
        if op not in job_conf:
            raise ConfigError('option "{}" is required'.format(op))

    # 检查枚举类型配置项
    enums = {
        'period': ('year', 'month', 'week', 'day', 'hour'),
        'is_active': ('true', 'false'),
    }
    for k, scope in enums.items():
        if job_conf[k] not in scope:
            raise ConfigError('option "{}" should be in "{}"'.format(k, list(scope)))

    # 渲染配置项
    try:
        job_conf = render_job_conf(job_conf)
    except Exception as e:
        raise ConfigError('failed rendering config: \n{}'.format(e))

    # 解析 is_active
    job_conf['is_active'] = True if job_conf['is_active'].lower() == 'true' else False

    # 解析 due_time
    try:
        job_conf['due_time'] = dateparser.parse(job_conf['due_time'])
    except:
        raise ConfigError('due_time {!r} can not be parsed'.format(job_conf['due_time']))

    # 编码 description
    if isinstance(job_conf['desc'], unicode):
        job_conf['desc'] = job_conf['desc'].encode('utf8')

    # # DUETIME 环境变量只允许用在 sql 选项中
    # for k, v in job_conf.items():
    #     if 'DUETIME' in v and k != 'sql':
    #         raise ConfigError(
    #             'environment variable "DUETIME" should only be used in option "sql"')

    # retry_times 应为整数
    try:
        job_conf['retry_times'] = int(job_conf['retry_times'])
    except ValueError as e:
        raise ConfigError(
            'option "retry_times" should be an integer, but {!r} got'.format(job_conf['retry_times']))

    # retry_interval 应该符合格式 HH:MM:SS
    m = re_time.match(job_conf['retry_interval'])
    if not m:
        raise ConfigError('option "retry_interval" should be in format of "HH:MM[:SS]"')

    # retry_interval 应能被解析成 datetime.timedelta
    try:
        parsed = dateparser.parse(job_conf['retry_interval'])
        job_conf['retry_interval'] = datetime.timedelta(
            hours=parsed.hour, minutes=parsed.minute, seconds=parsed.second)
    except ValueError as e:
        raise ConfigError(
            'can not parse retry_interval("{}") into datetime.timedelta'
            .format(job_conf['retry_interval']))

    # 将 db_conf, database, sql 分别处理成列表，并保证三者的长度等长（database 可为空）
    job_conf['db_conf'] = [s.strip() for s in job_conf['db_conf'].split(',') if s.strip()]
    if 'database' in job_conf and job_conf['database']:
        job_conf['database'] = [s.strip() for s in job_conf['database'].split(',') if s.strip()]
    else:
        job_conf['database'] = [None] * len(job_conf['db_conf'])
    job_conf['sql'] = [s.strip() for s in job_conf['sql'].split('::') if s.strip()]

    len1, len2, len3 = len(job_conf['db_conf']), len(job_conf['database']), len(job_conf['sql'])
    if len2 != 0 and len2 != len1:
        raise ConfigError('"db_conf" contains {} elements but "database" contains {}'.format(len1, len2))
    if len1 != len3:
        raise ConfigError('"db_conf" contains {} elements but "sql" contains {}'.format(len1, len3))

    # 检查 db_conf 是否合法
    for name in job_conf['db_conf']:
        if name not in db_configs.keys():
            raise ConfigError('Invalid db_conf {!r}, should be in {!r}'.format(
                job_conf['db_conf'], db_configs.keys()))

    # 如果 sql 选项是文件路径，那么从文件中读取 sql 内容，支持绝对路径与相对路径
    for i, s in enumerate(job_conf['sql']):
        if s.startswith('/') or s.startswith('~/') or s.startswith('.') or s[-4:].lower() in ('.sql', '.hql'):
            if not os.path.isfile(s):
                raise ConfigError('sql file not exists: {!r}'.format(s))
            with open(s, 'r') as f:
                # 文件中的 %(name)s 因为脱离了 cfg 文件，不会被自动替换，此处需手动置换
                sql = f.read()
                job_conf['sql'][i] = sql % job_conf

    # 检查 validator 是否有语法错误，以及是否调用了不存在的变量
    from context import get_validator_context
    try:
        # 此处仅用于快速检验校验表达式有无语法错误，不执行实际计算
        eval(job_conf['validator'], {'__builtins__': {}}, dict(get_validator_context(), result=None))
    except (SyntaxError, NameError) as e:
        tb = traceback.format_exc()
        raise ConfigError('error in option "validator", traceback is: \n{}'.format(tb))
    except:
        pass

    # # 对于历史数据监控（基于 claim 校验函数），如果用户未显式设置周期参数，则使用 period 选项填充
    # # 例如：'claim(result, gt(30))' --> 'claim(result, gt(30), period="day")'
    # if re.search(r'\bclaim\s*\(', job_conf['validator']):
    #     if 'period' not in job_conf['validator']:
    #         job_conf['validator'] = re.sub(
    #             r'(\bclaim\s*)\((.*)\)',
    #             r'\1(\2, period="{}")'.format(job_conf['period']),
    #             job_conf['validator'])

    # 从 db_configs 中取出对应的 db_conf 替换 db_conf 字段
    for i, name in enumerate(job_conf['db_conf']):
        job_conf['db_conf'][i] = db_configs[name]
        if job_conf['database'][i]:
            job_conf['db_conf'][i]['database'] = job_conf['database'][i]

    return job_conf


# 渲染配置
# ------------------------------------------------------------------------------
# 创建默认的 jinja2 Environment，使用 '{}' 标识变量渲染块
env = jinja2.Environment(
    variable_start_string='{',
    variable_end_string='}',)
env.filters = get_filter_context()

# 有些模板变量依赖其他选项，必须等其他选项渲染完成后才能渲染
DEPENDING_VARS = ('DUETIME', )

def _escape_vars(s, variables):
    """转义指定的一些变量，使其暂时不必渲染。
    此处简单地将花括号替换成了 \x01 \x02。
    """
    if isinstance(variables, basestring):
        variables = [variables]
    t = '|'.join(variables)
    return re.sub(r'{([^{}]*?(%s)[^{}]*?)}' % t, '\x01' + r'\1' + '\x02', s)

def _unescape_vars(s, variables):
    """_escape_vars 的逆操作"""
    return s.replace('\x01', '{').replace('\x02', '}')

def render_job_conf(job_conf):
    """载入时渲染"""
    global DEPENDING_VARS, env

    today = datetime.date.today()
    basetime = datetime.datetime.combine(today, datetime.time.min)

    env.globals = dict(BASETIME=basetime)

    for k, v in job_conf.items():
        if isinstance(v, basestring) and '{' in v:
            # 把依赖性变量暂时 escape 掉，渲染完成后再还原
            v = _escape_vars(v, DEPENDING_VARS)
            try:
                v = env.from_string(v).render()
            except Exception as e:
                raise ValueError('option `{}={}` render error: \n{}'.format(k, v, e))
            job_conf[k] = _unescape_vars(v, DEPENDING_VARS)

    return job_conf

def render_depending_job_conf(job_conf):
    """有一些选项依赖其他选项，必须等其他选项渲染之后才能渲染。
    例如小时级作业的 sql 选项中可能包含的 DUETIME 变量，只有在 due_time 选项渲染完成后才能确定。
    """
    global env

    env.globals = dict(DUETIME=job_conf['due_time'])

    # 目前仅 sql, validator 选项为依赖性渲染，后期可能增加别的选项
    for op in ('sql', 'validator'):
        if isinstance(job_conf[op], (list, tuple)):
            sep = '\x01'
            job_conf[op] = env.from_string(sep.join(job_conf[op])).render().split(sep)
        else:
            job_conf[op] = env.from_string(job_conf[op]).render()

    return job_conf


# 主函数，载入、处理所有配置并返回给主程序
# ------------------------------------------------------------------------------
def get_job_conf_list(db_config_file, job_config_files, job_names):
    """检查、清洗和渲染作业配置。
    返回一个处理好的作业配置序列，其中的每个作业配置可用于生成和运行作业。
    """

    # 由于作业配置文件可以有多个，所以需要判断其中有误冲突作业。
    # 如果作业名有冲突，报错告知具体冲突情况并退出程序。
    res = detect_configs_conflict(
        job_config_files,
        key=lambda s: s!='DEFAULT' and not s.startswith('_'))
    if res:
        raise ConfigError('Conflicted job name "{}" in "{}" and "{}"'.format(*res))

    # 读取数据库配置文件
    db_configs = get_config(db_config_file)
    for name, db_conf in db_configs.items():
        db_conf['_name'] = name
        try:
            db_conf['port'] = int(db_conf['port'])
        except ValueError:
            raise ConfigError('db-config error, port should be an integer, but {!r} got'.format(db_conf['port']))

    # 读取监控作业配置文件（注意，可以是多个文件）
    job_configs = get_config(job_config_files)

    # 如果不指定 job_names，则监控全部（但跳过 DEFAULT 和以下划线开头的作业）
    if not job_names:
        job_names = (
            name for name in job_configs.keys()
            if name != 'DEFAULT' and not name.startswith('_'))

    # 遍历处理作业
    for job_name in job_names:
        # 如果作业名称不存在，将直接报错并退出程序
        if not job_name in job_configs:
            raise ConfigError('Job name "{}" not exists'.format(job_name))

        # 获取 job_conf，并将 job 名称绑定到一个私有属性 _name 上
        job_conf = dict(job_configs[job_name], _name=job_name)

        # 检查作业配置，如果有误将打印错误并跳过该作业
        job_conf = check_out_job_config(job_conf, db_configs)
        if job_conf is None:
            continue

        # 跳过非活跃作业
        if not job_conf['is_active']:
            logger.info('skiped inactive job "{}"'.format(job_name))
            continue

        # 天级以上作业
        if job_conf['period'] != 'hour':
            # 如果 due_time 不是当天则跳过
            today = datetime.date.today()
            if job_conf['due_time'].date() != today:
                logger.info('skiped unscheduled job: [{}] at {}'.format(job_name, job_conf['due_time']))
                continue
            yield render_depending_job_conf(job_conf)

        # 小时级作业复制成 24 份
        else:
            one_hour = datetime.timedelta(hours=1)
            for i in range(24):
                due_time = job_conf['due_time'] + i * one_hour
                # 为了区分不同小时的同一个作业，在作业名称中加入时间
                name = job_conf['_name'] + '_hour' + due_time.strftime('%H')
                new_job_conf = dict(job_conf, due_time=due_time, _name=name)
                yield render_depending_job_conf(new_job_conf)
