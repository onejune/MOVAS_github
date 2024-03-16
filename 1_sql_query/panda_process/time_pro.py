#coding=utf-8
from datetime import datetime, timedelta
import time

today = datetime.today()#今天
yesterday_ts = time.time() - 24 * 3600#昨天
yesterday = datetime.fromtimestamp(yesterday_ts).strftime('%Y-%m-%d') #转换为年月日
fromtime =  (today -timedelta(16)).strftime('%Y-%m-%d') #16天前，并转化为年月日
day_before_yesterday_ts = yesterday_ts - 24*3600#前天
day_before_yesterday = datetime.fromtimestamp(day_before_yesterday_ts).strftime('%Y-%m-%d')


