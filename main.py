# -*- coding: utf-8 -*-

# import pandas
import os

from data_monitor.launch import execute


cur_dir = os.path.dirname(os.path.abspath(__file__))
db_config_file = os.path.join(cur_dir, 'database.cfg')
job_config_file = os.path.join(cur_dir, 'job.cfg')


if __name__ == '__main__':
    execute(db_config_file, job_config_file)
