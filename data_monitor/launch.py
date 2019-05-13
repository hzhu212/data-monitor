# -*- coding: utf-8 -*-

"""发起监控程序"""

import argparse
from collections import namedtuple
import concurrent.futures
from contextlib import closing
import datetime
import logging
import os
import re
import Queue
import time
import traceback

import pandas as pd

from .alarm import format_baidu_hi, send_baidu_hi, format_email, send_email
from .config import get_job_conf_list
from .context import get_validator_context
from .db import get_connection
from .util import ValidateFailInfo


logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(name)s %(levelname)s: %(message)s')
logger = logging.getLogger('data_monitor')


# 控制台打印表格时，如果行数超过 10，则使用省略号折叠
pd.set_option('display.max_rows', 10)
pd.set_option('display.width', 120)


def run_job(job):
    """执行一个作业。
    返回一个 2-tuple，两个字段分别代表：
        1. 是否校验成功；
        2. 附加消息对象，解释校验失败的原因，用于发送警报。
    """

    # 一个作业可能包含多个 SQL 查询
    results = []
    for db_conf, sql in zip(job['db_conf'], job['sql']):

        with closing(get_connection(db_conf)) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            # querys need to commit, except SELECT or SHOW query
            if not (sql[:7].upper() == 'SELECT ' or sql[:5].upper() == 'SHOW '):
                conn.commit()

            res = cursor.fetchall()
            # if result is only one element, then unpack it
            if len(res) == 1 and len(res[0]) == 1:
                res = res[0][0]

            # else return a list of namedtuples
            else:
                col_names = [t[0] for t in cursor.description]
                for i, name in enumerate(col_names):
                    if not re.match(r'^[\w_]+$', name):
                        col_names[i] = 'col' + str(i)
                RowType = namedtuple('RowType', col_names)
                res = [RowType(*tp) for tp in res]

            results.append(res)

    # if job has only one sql, then unpack the results as one result
    if len(results) == 1:
        results = results[0]

    context = get_validator_context()
    context.update({'result': results})
    ret = eval(job['validator'], {'__builtins__': {}}, context)

    try:
        # 如果用户 validator 中同时返回了 ok 和 info，则直接使用
        ok, info = ret

        # 确保 info 为 ValidateFailInfo 类型
        if not isinstance(info, ValidateFailInfo):
            try:
                info = ValidateFailInfo(*info)
            except:
                info = ValidateFailInfo(None, info)

    except (TypeError, ValueError):
        # 否则，认为用户只返回了 ok，使用 SQL 查询结果作为默认的 info。
        ok = ret
        info = ValidateFailInfo('default', results)

    return bool(ok), info


def main(db_config_file, job_config_files, job_names, pool_size=16, poll_interval=10):
    """主程序，处理作业排队、分发、重试逻辑。
    使用线程池以支持并行启动多个作业。
    """

    # 任务队列，使用优先队列，优先级为作业到期时间
    task_queue = Queue.PriorityQueue()

    logger.info('checking job configs ...')
    for job in get_job_conf_list(db_config_file, job_config_files, job_names):
        logger.info('job [{}] config OK.'.format(job['_name']))
        task_queue.put_nowait((job['due_time'], job))

    # # debug ...
    #     run_job(job)
    # return

    logger.info('all job configs OK.')
    logger.info('monitor start ...')
    logger.info('=' * 60)
    ntotal = task_queue.qsize()
    ncompleted = 0
    fs = {}
    logger.info('****** total jobs: {} ...'.format(ntotal))

    with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as executor:

        # 主循环，退出条件为作业队列为空，且线程池中没有残留作业
        while not task_queue.empty() or fs:
            logger.info('****** pending: {}, running: {}, completed: {} ******'.format(task_queue.qsize(), len(fs), ncompleted))

            # 如果任务队列非空，则轮询提取最近一个到期的作业，直到该作业到期
            if not task_queue.empty():
                due_time, job = task_queue.queue[0]
                now = datetime.datetime.now()
                if now >= due_time:
                    _, job = task_queue.get()
                    future = executor.submit(run_job, job)
                    fs[future] = job
                    logger.info('job [{}] is due. launched.'.format(job['_name']))
                else:
                    time.sleep(poll_interval)

            # 收集并处理执行完成的 job
            try:
                for future in concurrent.futures.as_completed(fs, timeout=poll_interval if task_queue.empty() else 0.001):
                    ncompleted += 1
                    job = fs.pop(future)
                    try:
                        ok, info_obj = future.result()
                    except Exception as e:
                        logger.error('job [{}] raised an exception:'.format(job['_name']))
                        logger.exception(e)
                        ok = False
                        info_obj = ValidateFailInfo('exception', traceback.format_exc())

                    # job 校验成功，打印日志，此 job 完成
                    if ok:
                        logger.info('job [{}] returned. status: OK.'.format(job['_name']))
                        continue

                    # job 校验失败，发送报警，尝试重试 job
                    baidu_hi_msg = format_baidu_hi(job, info_obj)
                    indented_msg = '\t' + baidu_hi_msg.replace('\n', '\n\t')
                    logger.info('job [{}] returned. status: =====> ALARM <=====\n{}'.format(job['_name'], indented_msg))
                    send_baidu_hi(job['alarm_hi'], baidu_hi_msg)
                    send_email(job['alarm_email'], format_email(job, info_obj))

                    if job['retry_times'] > 0:
                        job['retry_times'] -= 1
                        logger.info('job [{}] retrying. times left: {}.'.format(job['_name'], job['retry_times']))
                        task_queue.put_nowait((datetime.datetime.now() + job['retry_interval'], job))

            except concurrent.futures.TimeoutError:
                pass

        logger.info('****** pending: {}, running: {}, completed: {} ******'.format(task_queue.qsize(), len(fs), ncompleted))
        logger.info('all jobs ({}) finished.'.format(ntotal))
        logger.info('=' * 60)
        logger.info('monitor exit.')


def execute(default_db_config_file, default_job_config_file):
    """解析命令行参数并启动程序"""

    parser = argparse.ArgumentParser(
        description='data-monitor: monitor data of database and alarm when error occurs')
    parser.add_argument(
        '-c', '--config-file', dest='job_config_files', action='append',
        help='path of job config file, if not provided, use default config file. '
            'you can provide multiple config files by repeating `-c` option, '
            'job name conflicts will be auto-detected.')
    parser.add_argument(
        '--db-config-file', dest='db_config_file',
        help='path of database config file, if not provided, use default config file.')
    parser.add_argument(
        '-j', '--job', dest='job_names', action='append',
        help='job name (section name in your job config file). you can launch '
            'multiple jobs by repeating `-j` option.')
    parser.add_argument(
        '-f', '--force', dest='force_run', action='store_true',
        help='force to run all jobs, including inactive ones.')
    parser.add_argument(
        '--check', dest='run_check',  action='store_true',
        help='check job config(run job immediately, don\'t wait until job `start_at`.)')

    args = parser.parse_args()

    # 检查用户传入的配置文件是否存在
    if args.db_config_file:
        if not os.path.isfile(args.db_config_file):
            raise ValueError('database config file "{}" not exists'.format(args.db_config_file))
        db_config_file = args.db_config_file
    else:
        db_config_file = default_db_config_file

    if args.job_config_files:
        for file in args.job_config_files:
            if not os.path.isfile(file):
                raise ValueError('job config file "{}" not exists'.format(file))
        job_config_files = args.job_config_files
    else:
        job_config_files = [default_job_config_file]

    main(db_config_file, job_config_files, args.job_names)
