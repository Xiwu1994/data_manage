# -*- coding:utf-8 -*-
#Date的工具类

import datetime
import time

class DateUtil(object):

    def get_current_date(self,format='%Y-%m-%d'):
        current_date = datetime.datetime.strftime(datetime.datetime.now(),format);
        return current_date

    def get_last_n_date(self,format='%Y-%m-%d',days=1):
        today = datetime.datetime.now()
        last_date = today-datetime.timedelta(days=days)
        return datetime.datetime.strftime(last_date,format)

    def get_yesterday(self):
        return  self.get_last_n_date(days=1)

    def get_the_day_before_yesterday(self):
        return self.get_last_n_date(days=2)

    # 10位的时间戳
    def convert_timestamp_to_date(self,timestamp):
        time_local = time.localtime(timestamp)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return dt
