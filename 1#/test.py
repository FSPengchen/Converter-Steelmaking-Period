import statistics
import pandas as pd
import once_Converter_Cycle
import time
import pymysql
from dbutils.pooled_db import PooledDB
import datetime
from decimal import *
from clickhouse_driver import Client
# import struct_convert
import Date_Time_Arithmetic
import numpy as np
import statistics

# 中台数据库连接
clickhouse_user = 'guest1'
clickhouse_pwd = '0ecadf'
clickhouse_host_sq = '10.6.80.18'
clickhouse_database = 'sensor'
client = Client(host=clickhouse_host_sq, user=clickhouse_user, database=clickhouse_database,
                password=clickhouse_pwd)
print('中台数据库连接成功')

# mysql数据库连接
pool = PooledDB(pymysql, 1, host='10.6.1.82', user='furnace', passwd='furnace', db='120Tconverter1', port=3306)
conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
cur = conn.cursor()
# cur.execute(sql)
# conn.commit()
# conn.close()
print('mysql数据库连接成功')

# 记录文本
file_handle = open('err.txt', mode='a')
file_handle.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ':\n提示:\n' + str('记录文本连接成功') + '\n')
print('记录文本连接成功')

test_list = []

def sql_weight_value(start_time, end_time, eid):
    sql = "SELECT DISTINCT toString(EventTime, 'Asia/Shanghai') as timeID ,`50` FROM sensor.process_param WHERE EventTime >= '" + \
          str(start_time) + "' and EventTime <= '" + str(end_time) + "' AND  Eid='" + eid + "';"
    print('采集炉号SQL',sql)
    sql_values = client.execute(sql)
    print('采集炉号返回值', len(sql_values), sql_values,)
    print('print',sql_values[0][1])
    values_list = []
    if sql_values:
        for n in range(len(sql_values)):
            print(n,sql_values[n][1],round(sql_values[n][1],1))
            values_list.append(round(sql_values[n][1],1))
        print(len(values_list),values_list)
        sql_values = statistics.mode(values_list)
    else:
        sql_values = '0'
    return sql_values


end_time = "2022-06-21 00:37:25"
start_time = "2022-06-21 00:38:25"
eid = '8sx1j18z'
# print(sql_weight_value(start_time, end_time, eid))



my_list = [113.6, 109.9, 109.9, 105.3, 101.5, 101.5, 97.1, 93.2, 93.2, 88.5, 84.9, 84.9, 81.9, 78.4, 75.8, 75.8, 73.3, 70.6, 70.6, 68.7, 66.7, 66.7, 65.4, 64.1, 64.1, 62.4, 62.4, 60.7, 60.7, 59.2, 59.2, 59.2, 57.6, 57.6, 56.3, 56.3, 55.1, 55.1, 54.5, 54.8, 54.4, 54.4, 54.4, 54.4, 55.8, 55.8, 57.6, 60.2, 60.2, 61.6, 63.2, 63.2, 64.4, 64.4, 65.4, 65.4, 65.4, 67.1, 67.1, 68.5, 68.5]
ser = pd.Series(my_list) * 10

my_list = ser.to_list()

# mode1 = statistics.mode(my_list)
mode2 = np.argmax(np.bincount(my_list))
print(mode2)
mode1 = (pd.Series(my_list) / 10).to_list()
print(mode1)
# print(mode2)

conn.close()
