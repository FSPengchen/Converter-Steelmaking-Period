"""

一段底吹时间
相当于  120吨2号转炉钢包出钢位底吹操作时间
mw128
二段底吹
就是120吨2号转炉钢包氩站二段底吹时间
mw138
三段底吹
就是120吨2号转炉钢包氩站三段底吹时间
mw140

"""

import time
import statistics
import pymysql
import pandas as pd
import numpy as np
from dbutils.pooled_db import PooledDB
import datetime
from decimal import *
from clickhouse_driver import Client
# import Date_Time_Arithmetic
from Date_Time_Arithmetic import *

# 中台数据库连接
clickhouse_user = 'oa'
clickhouse_pwd = 'oa'
clickhouse_host_sq = '10.6.80.47'
clickhouse_database = 'power'
client = Client(host=clickhouse_host_sq, user=clickhouse_user, database=clickhouse_database, password=clickhouse_pwd)
print('中台数据库连接成功')

# mysql数据库连接
pool = PooledDB(pymysql, 1, host='10.6.1.82', user='furnace', passwd='furnace', db='120Tconverter2', port=3306)
conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
cur = conn.cursor()
# cur.execute(sql)
# conn.commit()
# conn.close()
print('mysql数据库连接成功')

smeltv1 = {
    'inDate': "0",
    'heatNo': "0",
    'argon_begin': "0",
    'argon_end': "0",
    'argon_oneBlow_time': "0",
    'argon_secBlow_time': "0",
    'argon_terBlow_time': "0",
    'argon_station_Blow_freq': "0",

}


# region 函数

def wirte_sql():
    print(smeltv1['argon_begin'][:10], smeltv1['inDate'])
    if smeltv1['inDate'] == str(smeltv1['argon_begin'][:10]) and smeltv1['heatNo'] != 0:
        sql_str = ''
        sql_value = ''
        for i in smeltv1.keys():
            # print(i)
            sql_str = sql_str + i + ","
        for i in smeltv1.values():
            # print(i)
            sql_value = sql_value + "'" + str(i) + "',"
        sql = "replace into smeltinfo_argon (" + sql_str[:-1] + ")value(" + sql_value[:-1] + ")"

        print(sql)
        cur.execute(sql)
        conn.commit()
        print('写入氩气周期82数据库')


# 吹氩炉次开始时间
def sql_start_time(day, heatNo, eid):
    sql = "SELECT   toString(ts, 'Asia/Shanghai') as timeid FROM power.collect_steelmaking  WHERE  ts >= '" + day + "' and v= '" + str(
        heatNo) + "'  and  eid='" + eid + "'  ORDER BY ts asc LIMIT 1 ;"
    # print(sql)
    sql_values = client.execute(sql)
    if sql_values:
        sql_values = sql_values[0][0]
        # print(heatNo,'开始时间',sql_values,type(sql_values))
    else:
        sql_values = None
    return sql_values


# 吹氩炉次结束时间
def sql_end_time(day, heatNo, eid):
    sql = "SELECT   toString(ts, 'Asia/Shanghai') as timeid FROM power.collect_steelmaking  WHERE  ts >= '" + day + "' and v= '" + str(
        heatNo) + "'  and  eid='" + eid + "'  ORDER BY ts desc LIMIT 1 ;"
    # print(sql)
    sql_values = client.execute(sql)
    if sql_values:
        sql_values = sql_values[0][0]
        # print(heatNo,'结束时间',sql_values,type(sql_values))
    else:
        sql_values = None
    return sql_values


# 吹氩炉次结束时间
def sql_max_value(start_time, end_time, eid):
    sql = "SELECT  v FROM power.collect_steelmaking  WHERE  ts > '" + start_time + "' and ts < '" + end_time + "' and  eid='" + eid + "'  ORDER BY ts ;"
    # print(sql)
    sql_values = client.execute(sql)
    print('范围取值', sql_values)
    if sql_values:
        # sql_values = int(list(max(sql_values))[0])
        sql_values = int(max(sql_values, key=lambda x: x[0])[0])
        # print('最大值时间',sql_values,type(sql_values))
    else:
        sql_values = '0'
    return sql_values




# endregion

flag = 1  # 循环状态
# 主程序
while flag:
    # days = 26
    # for day in range(days):
    # try:
    # region 设定时间
    date_today = datetime.date.today().strftime("%Y-%m-%d")  # 目前日期 str
    # date_today = '2023-04-05'  # 设定执行日期
    # date_today = Date_Time_Arithmetic.Add_strday_str(date_today, 1)  # 循环每次增加一日
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 目前时间 str

    print('今日日期:', date_today)
    print('现在时间:', time_now, type(time_now))
    # date_today = "2023-02-20"

    # 获取当日的钢包吹氩炉号
    sql = "SELECT DISTINCT v FROM power.collect_steelmaking  WHERE ts >= '" + date_today + "' AND  eid='e42fe0ce'  ORDER BY ts asc;"
    print(sql)

    try:
        sql_values = client.execute(sql)
    except Exception as e:
        print(e)

    if sql_values:
        # 获取元组
        print(len(sql_values))
        for i in sql_values:
            print(i)
            # 前一日最后一笔炉号
            sql = "SELECT  v FROM power.collect_steelmaking  WHERE  ts >= '" + Add_strday_str(date_today,
                                                                                              -1) + "' and ts <= '" + date_today + "' and eid='e42fe0ce'  ORDER BY ts desc LIMIT 1 ;"
            # print(sql)
            day_last_heatNo = client.execute(sql)
            if day_last_heatNo:
                day_last_heatNo = day_last_heatNo[0][0]
                print('前一天的最后一笔炉号', day_last_heatNo)

            if day_last_heatNo == i[0]:
                print('<前一天的最后一笔炉号>与<当日第一笔炉号>相同', day_last_heatNo)
                smeltv1['inDate'] = Add_strday_str(date_today, -1)
                smeltv1['heatNo'] = int(i[0])
                smeltv1['argon_begin'] = sql_start_time(Add_strday_str(date_today, -1), i[0], 'e42fe0ce')
                print('炉次:', i[0], 'argon_begin:', smeltv1['argon_begin'])

                smeltv1['argon_end'] = sql_end_time(date_today, i[0], 'e42fe0ce')
                print('炉次:', i[0], 'argon_end:', smeltv1['argon_end'])

                smeltv1['argon_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '03f5aeb1')
                print('炉次:', i[0], 'argon_time:', smeltv1['argon_time'])

                smeltv1['argon_oneBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], 'b89036c4')
                print('炉次:', i[0], 'argon_oneBlow_time:', smeltv1['argon_oneBlow_time'])

                smeltv1['argon_secBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], 'a7f98ad9')
                print('炉次:', i[0], 'argon_secBlow_time:', smeltv1['argon_secBlow_time'])

                smeltv1['argon_terBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '74be0e74')
                print('炉次:', i[0], 'argon_terBlow_time:', smeltv1['argon_terBlow_time'])

                smeltv1['argon_station_Blow_freq'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '45d2ee98')
                print('炉次:', i[0], 'argon_station_Blow_freq:', smeltv1['argon_station_Blow_freq'])

                wirte_sql()

            else:
                print('吹氩炉次', i[0])
                smeltv1['inDate'] = date_today
                smeltv1['heatNo'] = int(i[0])
                smeltv1['argon_begin'] = sql_start_time(date_today, i[0], 'e42fe0ce')
                print('炉次:', i[0], 'argon_begin:', smeltv1['argon_begin'])
                smeltv1['argon_end'] = sql_end_time(date_today, i[0], 'e42fe0ce')
                print('炉次:', i[0], 'argon_end:', smeltv1['argon_end'])
                smeltv1['argon_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '03f5aeb1')
                print('炉次:', i[0], 'argon_time:', smeltv1['argon_time'])

                smeltv1['argon_oneBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], 'b89036c4')
                print('炉次:', i[0], 'argon_oneBlow_time:', smeltv1['argon_oneBlow_time'])

                smeltv1['argon_secBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], 'a7f98ad9')
                print('炉次:', i[0], 'argon_secBlow_time:', smeltv1['argon_secBlow_time'])

                smeltv1['argon_terBlow_time'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '74be0e74')
                print('炉次:', i[0], 'argon_terBlow_time:', smeltv1['argon_terBlow_time'])

                smeltv1['argon_station_Blow_freq'] = sql_max_value(Addsec_dateTime_str(smeltv1['argon_begin'], 1),
                                                      smeltv1['argon_end'], '45d2ee98')
                print('炉次:', i[0], 'argon_station_Blow_freq:', smeltv1['argon_station_Blow_freq'])

                wirte_sql()

    time.sleep(10)

conn.close()
