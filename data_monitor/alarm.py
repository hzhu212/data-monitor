# -*- coding: utf-8 -*-

"""
@Author:      zhuhe02
@Email:       zhuhe02@baidu.com
@Description: 报警模块。实现短信、邮件、百度Hi多种报警方式
@CreateAt:    2019-04-01
"""


import logging
import os


cur_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(cur_dir, 'templates')

logger = logging.getLogger(__name__)


def format_baidu_hi(job, info):
    """生成百度 hi 消息。
    info 是一个 2-tuple (见 util.ValidateFailInfo)，两个字段的含义分别为：type、content。
    """
    try:
        type_, content = info
    except:
        type_ = None
        content = info

    # 配置错误警报。由于任何配置项都可能缺失，因此要保证最精简的提示信息（只包括 job name）
    if type_ == 'config_error':
        msg = [
            '作业名称：{}'.format(job['_name']),
            '=' * 20,
            '报警原因：作业配置错误',
            '-' * 20,
            str(content), ]
        return '\n'.join(msg)

    msg = [
        '🙏\n监控描述：{}'.format(job['desc']),
        '作业名称：{}'.format(job['_name']),
        '发起时间：{}'.format(job['due_time']),
        '=' * 20, ]

    if type_ == 'claim':
        # claim 类型对应的 content 为 pandas.DataFrame
        try:
            content_s = content.to_string(max_rows=10).encode('utf8')
        except:
            content_s = str(content)
        msg += [
            '报警原因：数据缺失或不符合要求',
            '校验表达式：`{}`'.format(job['validator'].encode('utf8')),
            '-' * 20,
            '不合格的数据：',
            content_s,]

    elif type_ == 'diff':
        # diff 类型对应的 content 为 pandas.DataFrame
        try:
            content_s = content.to_string(max_rows=10).encode('utf8')
        except:
            content_s = str(content)
        msg += [
            '报警原因：数据diff超出阈值',
            '校验表达式：`{}`'.format(job['validator'].encode('utf8')),
            '-' * 20,
            '不合格的数据：',
            content_s,]

    elif type_ == 'exception':
        # exception 类型对应的 content 为一个字符串，包含错误堆栈
        msg += [
            '报警原因：作业抛出异常',
            '-' * 20,
            content, ]

    else:
        # 默认消息类型对应的 content 为 sql 查询结果
        msg += [
            '报警原因：数据校验未通过',
            '-' * 20,
            '校验表达式：`{}`'.format(job['validator'].encode('utf8')),
            '查询结果`result`：`{}`'.format(repr(content)),]

    msg = '\n'.join(msg)
    return msg


def format_email(job, info):
    """生成 html 邮件。对不同的消息类型使用不同的消息模板。
    info 是一个 2-tuple (见 util.ValidateFailInfo)，两个字段的含义分别为：type、content。
    """
    try:
        type_, content = info
    except:
        type_ = None
        content = info

    if type_ == 'config_error':
        template_file = os.path.join(template_dir, 'config_error.html')
        content = str(content).replace('\t', ' '*4).replace(' ', '&nbsp;').replace('\n', '</p><p>')
        content = '<p>' + content + '</p>'
        with open(template_file, 'r') as f:
            msg = f.read().format(job=job, content=content)
            return msg

    if type_ == 'claim':
        template_file = os.path.join(template_dir, 'claim.html')
        try:
            content = content.to_html().encode('utf8')
        except AttributeError:
            content = str(content)

    elif type_ == 'diff':
        template_file = os.path.join(template_dir, 'diff.html')
        try:
            content = content.to_html().encode('utf8')
        except AttributeError:
            content = str(content)

    elif type_ == 'exception':
        template_file = os.path.join(template_dir, 'exception.html')
        content = content.replace('\t', ' '*4).replace(' ', '&nbsp;').replace('\n', '</p><p>')
        content = '<p>' + content + '</p>'

    else:
        template_file = os.path.join(template_dir, 'default.html')

    htmled_sql = '<hr/>'.join('<p>' + s.replace('\n', '</p><p>') + '</p>' for s in job['sql'])
    with open(template_file, 'r') as f:
        msg = f.read().format(
            job=dict(job, validator=job['validator'].encode('utf8')),
            content=content,
            sql=htmled_sql,
            database=', '.join(db['_name'] for db in job['db_conf'])
            )

    return msg


def send_baidu_hi(to_users, msg):
    """向百度Hi用户发送消息。
    百度Hi每条消息最多支持 2KB，超长的消息需要做分割处理。
    """

    import requests

    def split_msg(msg, blocksize=2048):
        """将消息切块。
        照顾到用户体验，尽量在换行处切割。如果一行内容超长，就只能在行内切割了。
        """
        if len(msg) <= blocksize:
            return [msg]
        chunk, msg = msg[:blocksize], msg[blocksize:]
        index = chunk.rfind('\n')
        if index == -1:
            return [chunk] + split_msg(msg, blocksize=blocksize)
        return [chunk[:index+1], split_msg(chunk[index+1:] + msg, blocksize=blocksize)]

    msg = str(msg)
    api_url = 'http://xp2.im.baidu.com/ext/1.0/sendMsg'
    data = {
        'access_token': '75d563d4277817b3c5e755ee8b164630',
        'msg_type': 'text',
    }

    for user in to_users:
        data['to'] = user
        for m in split_msg(msg, 2048):
            data['content'] = m
            r = requests.post(api_url, data=data)
            res = r.json()
            if not res['result'].lower() == 'ok':
                logger.error('failed sending BaiduHi message to user "{}". response: {!r}'.format(user, res))
                break
        else:
            logger.info('succeeded sending BaiduHi message to user "{}"'.format(user))


def send_email(to_users, msg):
    """向邮箱发送信息"""

    from email.header import Header
    from email.mime.text import MIMEText
    import smtplib

    msg = str(msg)
    from_addr = 'ssg_oppd_ta@baidu.com'
    to_users = [s.strip() if '@' in s else s.strip() + '@baidu.com' for s in to_users]

    mail_type = 'plain'
    if '</' in msg and '>' in msg:
        mail_type = 'html'
    mail = MIMEText(msg, mail_type, 'utf-8')
    mail['From'] = from_addr
    mail['To'] = ','.join(to_users)
    mail['Subject'] = Header('数据监控警报', 'utf-8').encode()

    server = smtplib.SMTP('proxy-in.baidu.com', 25)
    server.sendmail(from_addr, to_users, mail.as_string())
    server.quit()
