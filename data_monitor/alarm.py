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

logger = logging.getLogger('data_monitor.alarm')


def wrap_message(job, info):
    """生成漂亮的 message。
    可根据消息类型进行不同的加工，比如生成 html 邮件等。

    info 是一个 2-tuple (见 util.ValidateFailInfo)，两个字段的含义分别为：type、content。
    """
    try:
        type_, content = info
    except:
        type_ = None
        content = info

    if type_ == 'diff':
        template_file = os.path.join(template_dir, 'diff.html')
        try:
            content = content.to_html().encode('utf-8')
        except AttributeError:
            content = str(content)

    elif type_ == 'exception':
        template_file = os.path.join(template_dir, 'exception.html')
        content = content.replace(' ', '&nbsp;').replace('\n', '</p><p>')
        content = '<p>' + content + '</p>'
        # content = content.replace('\n', '&#13;&#10;')

    else:
        template_file = os.path.join(template_dir, 'default.html')
        content = str(content)

    with open(template_file, 'r') as f:
        msg = f.read().format(job_name=job['_name'], due_time=job['due_time'], content=content)

    # print(msg)
    return msg


def send_baidu_hi(to_users, msg):
    """向百度Hi用户发送消息"""

    import requests

    msg = str(msg)
    api_url = 'http://xp2.im.baidu.com/ext/1.0/sendMsg'
    data = {
        'access_token': '75d563d4277817b3c5e755ee8b164630',
        'msg_type': 'text',
        'content': msg,
    }

    for user in to_users:
        data['to'] = user
        r = requests.post(api_url, data=data)
        res = r.json()
        if res['result'].lower() == 'ok':
            logger.info('succeeded sending BaiduHi message to user "{}"'.format(user))
        else:
            logger.error('failed sending BaiduHi message to user "{}". response: {!r}'.format(user, res))


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
    mail['Subject'] = Header('发送自数据监控工具', 'utf-8').encode()

    server = smtplib.SMTP('proxy-in.baidu.com', 25)
    server.sendmail(from_addr, to_users, mail.as_string())
    server.quit()
