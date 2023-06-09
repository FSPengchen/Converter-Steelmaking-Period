import statistics
import time
import numpy as np
import pandas as pd
import pymysql
from dbutils.pooled_db import PooledDB
import datetime
from decimal import *
from clickhouse_driver import Client
# import struct_convert
import Date_Time_Arithmetic

smeltv1 = {
    'inDate': "0",
    'heatNo': "0",
    'heatAge': "0",
    'wateScrap_begin': "0",
    'wateScrap_end': "0",
    'wateScrap': "0",
    'addScrap_begin': "0",
    'addScrap_end': "0",
    'addScrap': "0",
    'waitIron_begin': "0",
    'waitIron_end': "0",
    'waitIron': "0",
    'addIron_begin': "0",
    'addIron_end': "0",
    'addIron': "0",
    'lowerOxy_begin': "0",
    'lowerOxy_end': "0",
    'lowerOxy': "0",
    'supplyOxy_begin': "0",
    'supplyOxy_end': "0",
    'supplyOxy': "0",
    'raiseOxy_begin': "0",
    'raiseOxy_end': "0",
    'raiseOxy': "0",
    'tempMeasur_begin': "0",
    'tempMeasur_end': "0",
    'tempMeasure': "0",
    'lowerTwoOxy_begin': "0",
    'lowerTwoOxy_end': "0",
    'lowerTwoOxy': "0",
    'supplyTwoOxy_begin': "0",
    'supplyTwoOxy_end': "0",
    'supplyTwoOxy': "0",
    'raiseTwoOxy_begin': "0",
    'raiseTwoOxy_end': "0",
    'raiseTwoOxy': "0",
    'moltenSteel_begin': "0",
    'moltenSteel_end': "0",
    'moltenSteel': "0",
    'lowerNitrogen_begin': "0",
    'lowerNitrogen_end': "0",
    'lowerNitrogen': "0",
    'splashSlag_begin': "0",
    'splashSlag_out': "0",
    'splashSlag': "0",
    'raiseNitrogen_begin': "0",
    'raiseNitrogen_end': "0",
    'raiseNitrogen': "0",
    'pourSlag_begin': "0",
    'pourSlag_end': "0",
    'pourSlag': "0",
    'smeltPeriod_begin': "0",
    'smeltPeriod_end': "0",
    'smeltPeriod': "0",
    'beforSteelTemp1': "0",
    'beforSteelTemp2': "0",
    'ironMelt': "0",
    'steelScrap': "0",
    'shove': "0",
    'gross': "0",
    'tare': "0",
    'item': "0",
    'hopperWeightHigh1Day': "0",
    'hopperWeightHigh2Day': "0",
    'hopperWeightHigh3Day': "0",
    'hopperWeightHigh4Day': "0",
    'hopperWeightHigh5Day': "0",
    'hopperWeightHigh6Day': "0",
    'hopperWeightHigh7Day': "0",
    'hopperWeightHigh8Day': "0",
    'hopperWeightHigh9Day': "0",
    'hopperWeightHigh10Day': "0",
    'hopperWeightMiddle1Day': "0",
    'hopperWeightMiddle2Day': "0",
    'hopperWeightMiddle3Day': "0",
    'hopperWeightMiddle4Day': "0",
    'hopperWeightMiddle5Day': "0",
    'hopperWeightMiddle6Day': "0",
    'hopperWeightMiddle7Day': "0",
    'hopperWeightMiddle8Day': "0",
    'alloyHigh1': "0",
    'alloyHigh2': "0",
    'alloyMiddle1': "0",
    'alloyMiddle2': "0",
    'alloyMiddle3': "0",
    'alloyMiddle4': "0",
    'alloyMiddle5': "0",
    'alloyMiddle6': "0",
    'alloyMiddle7': "0",
    'alloyMiddle8': "0",
    'material1': "0",
    'material2': "0",
    'material3': "0",
    'material4': "0",
    'material5': "0",
    'material6': "0",
    'material7': "0",
    'material8': "0",
    'castingNo': "0",
    'repair_begin': "0",
    'repair_end': "0",
    'repair': "0",
    'maintain_begin': "0",
    'maintain_end': "0",
    'maintain': "0",
    'clearFurnace_begin': "0",
    'clearFurnace_end': "0",
    'clearFurnace': "0",
    'clearSlag_begin': "0",
    'clearSlag_end': "0",
    'clearSlag': "0",
    'plan_begin': "0",
    'plan_end': "0",
    'plan': "0",
    'temporary_begin': "0",
    'temporary_end': "0",
    'temporary': "0",
    'converterGasRecoveryTime': "0",
    'lanceAge': '0',
    'cycle': '0',
    'addScrap_interval': "0",
    'addScrap_cycle': "0",
    'addIron_interval': "0",
    'addIron_cycle': "0",
    'lowerOxy_interval': "0",
    'lowerOxy_cycle': "0",
    'lowerTwoOxy_interval': "0",
    'lowerTwoOxy_cycle': "0",
    'tempMeasur_interval': "0",
    'tempMeasur_cycle': "0",
    'moltenSteel_interval': "0",
    'moltenSteel_cycle': "0",
    'lowerNitrogen_interval': "0",
    'lowerNitrogen_cycle': "0",
    'pourSlag_interval': "0",
    'GrossScrapWeight103': "0",
    'ScrapTare103': "0",
    'ScrapWeight103': "0",
    'GrossScrapWeight104': "0",
    'ScrapTare104': "0",
    'ScrapWeight104': "0",
    'GrossIronWeight101': "0",
    'IronTare101': "0",
    'IronWeight101': "0",
    'GrossIronWeight102': "0",
    'IronTare102': "0",
    'IronWeight102': "0",
    'smeltPeriod_OxygenWastage': "0",
    'smeltPeriod_NitrogenWastage': "0",
    'OxylanceMainPressure': "0",
    'OxylanceBranchMainPressure': "0",
    'Class': "0",
    'Team': "0",
    'steelType': "0",
    'steelSpecs': "0",
    'ironLoading': "0",
    'steelLoading': "0",
    'double_slag_state': "0",
    'sublance_temperature':"0",
    'repair_converter':"0",
    'desulfurization_lance_min_position': "0",
    'desulfurization_lance_begin': "0",
    'desulfurization_lance_end': "0",
}


start_time_list = []  # 存储每炉开始时间数组
end_time_list = []  # 存储每炉结束时间数组
STOP_start_time_list = []  # 存储停炉每炉开始时间数组
STOP_end_time_list = []  # 存储停炉结束时间数组
temp_everday_first_cycle_start_time = ''
everday_first_cycle_start_time = None
everday_first_stat = 0  # 每日第一炉的判断状态。
'''
everday_first_stat :查询第一笔数据过程的状态
0:初始化状态
1:RUN比STOP晚,即0->1,此炉为今日第一炉,已获取当日第一炉时间
2:RUN比STOP早,即1->0,下一炉为今日第一炉(过程)
3:过12点,上一炉未结束,待上一日炉次结束 1->0 ,再查找 0->1的第一笔时间,查找到当日第一炉
4:前日炉次结束,今日炉次未开始
5:正在昨日最后一炉生产中
6.当日第一炉次开始时间不是当日
'''

process_state = 0  # 过程步序
'''
process_state: 过程段
10: 获取第一炉时间完成
11: 未获取到炉次开始时间
12: 判断时间数组的异常错误
20: 获取炉次开始时间数组

30: 获取开始时间数组与结束时间数组

500: 还在执行前一日最后一炉过程
510: 前一日炉次结束，当日第一炉未开始

65535:重新进入循环

'''

class once_main():
    def __init__(self,client,cur,conn,date_today):
        print('进入前一日一次触发')
        self.flag = 1  # 循环状态
        self.days = 1
        self.client = client
        self.cur = cur
        self.conn = conn
        self.date_today = date_today
        self.main()


    # region 调用SQL函数

    # 读取炉次周期状态里第一笔到来的时间
    def sql_start_time(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts < '" + str(end_time_list[i]) + "' AND  eid='" + eid + "' and v = 1  ORDER BY ts asc LIMIT 1;"
        sql_values = self.client.execute(sql)
        print(sql)
        if sql_values:
            sql_values = sql_values[0][0]
        else:
            sql_values = None
        return sql_values

    # 读取炉次周期状态里最后一笔到来的时间
    def sql_end_time(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts < '" + str(end_time_list[i]) + "' AND  eid='" + eid + "' and v = 1  ORDER BY ts desc LIMIT 1;"
        sql_values = self.client.execute(sql)
        if sql_values:
            sql_values = sql_values[0][0]
        else:
            sql_values = None
        return sql_values

    # 炉次结束时间，其他状态的计时时间
    def sql_end_time_value(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts < '" + str(end_time_list[i]) + "' AND  eid='" + eid + "'  ORDER BY ts desc LIMIT 1;"
        # print('采集炉号SQL',sql)
        sql_values = self.client.execute(sql)
        # print('采集炉号返回值', sql_values)
        if sql_values:
            sql_values = sql_values[0][1]
        else:
            sql_values = '0'
        return sql_values

    # 2023.05.04 炉次结束时间加15分钟，获取氩后温度
    def sql_end_time_temperature_after_argon(self,i, eid):
        sql = "SELECT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts < '" + str(
            Date_Time_Arithmetic.Addmin_dateTime_str(str(end_time_list[i]),
                                                     15)) + "' AND  eid='" + eid + "'  ORDER BY ts desc LIMIT 1;"
        # print('采集炉号SQL',sql)
        try:
            sql_values = self.client.execute(sql)
        except Exception as e:
            print(e)
            sql_values = '0'

        # print('采集炉号返回值', sql_values)
        if sql_values:
            sql_values = sql_values[0][1]
        else:
            sql_values = '0'
        return sql_values

    # 炉次结束时间到下一炉时间内，其他状态的计时时间
    def sql_stop_time_value(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(STOP_start_time_list[i]) + "' and ts < '" + str(STOP_end_time_list[i]) + "' AND  eid='" + eid + "'  ORDER BY ts desc LIMIT 1;"
        # print('采集炉号SQL',sql)
        sql_values = self.client.execute(sql)
        # print('采集炉号返回值', sql_values)
        if sql_values:
            sql_values = sql_values[0][1]
        else:
            sql_values = '0'
        return sql_values

    # 指定时间的数据
    def sql_set_time_value(self, time, eid):
        if time != None and time != 'None':
            sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE  ts <= '" + str(time) + "' AND  eid='" + eid + "'  ORDER BY ts desc LIMIT 1;"
            print('采集炉号SQL', sql)
            sql_values = self.client.execute(sql)
            print('采集炉号返回值', sql_values)
            if sql_values:
                sql_values = sql_values[0][1]
            else:
                sql_values = '0'
        else:
            sql_values = '0'
        return sql_values

    # 炉次结束时间到下一炉时间内，其他状态的计时时间,结束时间+2秒(网络延时)
    def sql_stop_time_plusone_value(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts < '" + str(Date_Time_Arithmetic.Addsec_dateTime_str(str(end_time_list[i]),2)) + "' AND  eid='" + eid + "'  ORDER BY ts desc LIMIT 1;"
        # print('采集炉号SQL',sql)
        sql_values = self.client.execute(sql)
        # print('采集炉号返回值', sql_values)
        if sql_values:
            sql_values = sql_values[0][1]
        else:
            sql_values = '0'
        return sql_values

    # 炉次周期外
    def sql_stop_start_time(self,i, eid):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts > '" + \
              str(start_time_list[i]) + "' and ts < '" + str(start_time_list[
                  i + 1]) + "' AND  eid='" + eid + "' and v = 1  ORDER BY ts asc LIMIT 1;"
        sql_values = self.client.execute(sql)
        if sql_values:
            sql_values = sql_values[0][0]
        else:
            sql_values = None
        return sql_values

    # 煤气回收时间
    def sql_gasrecovery_time(self,i, eid1, eid2, eid3):
        # 在炉次周期内，判断二次状态有否有，获取结束时间。
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time_list[i]) + "' and ts <= '" + str(end_time_list[
                  i]) + "' AND  eid='" + eid2 + "' and v = 1  ORDER BY ts desc LIMIT 1;"
        print('在炉次周期内，判断二次状态有否有，获取最后的时间:', sql)
        sql_values = self.client.execute(sql)
        if sql_values:
            # 二次的结束时间
            sql_values = sql_values[0][0]
        else:
            # 没有二次，判断一次吹炼状态
            sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
                  str(start_time_list[i]) + "' and ts <= '" + str(end_time_list[
                      i]) + "' AND  eid='" + eid1 + "' and v = 1  ORDER BY ts desc LIMIT 1;"
            print('在炉次周期内，判断一次状态有否有，获取最后的时间:', sql)
            sql_values = self.client.execute(sql)
            if sql_values:
                # 二次的结束时间
                sql_values = sql_values[0][0]
            else:
                # 没有吹炼状态
                sql_values = 0

        if sql_values != 0:
            sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
                  str(start_time_list[i]) + "' and ts <= '" + str(
                sql_values) + "' AND  eid='" + eid3 + "' ORDER BY ts desc LIMIT 1;"
            print('在炉次周期内，炉次开始时间到 吹炼结束时间，获取获取的值:', sql)
            sql_values = self.client.execute(sql)
            if sql_values:
                # 吹炼结束时间的值
                sql_values = sql_values[0][1]
            else:
                # 没有吹炼状态
                sql_values = 0

        return sql_values

    # 递归查看压枪时间
    def desulfurization_lance_time(self,start_time, end_time):
        # # 在这里增加终点关氧前1分钟，1.55米（7845）以下停留时间
        # # 第一次供氧状态=1  大于10分钟 取值，  小于10分钟为0   供氧结束前 1分半的 枪位低于 1550  的有效时间总和,  到供氧结束时间或者 大于1550。   只记录一次时间
        sql = "SELECT ts, v FROM power.collect_steelmaking WHERE ts >= '" + str(start_time) + "' AND ts <= '" + str(
            end_time) + "' AND eid = '0082c988' AND v = 0 ORDER BY ts desc LIMIT 1"
        print('在供氧状态内是否有第二次吹氧', sql)
        sql_values = self.client.execute(sql)
        print('获取的值', sql_values)
        if sql_values:
            print(sql_values[0][0], type(sql_values[0][0]))
            self.desulfurization_lance_time(Date_Time_Arithmetic.Addsec_dateTime_str(str(sql_values[0][0]), 1), end_time)

        else:
            sql = "SELECT ts, v FROM power.collect_steelmaking WHERE ts >= '" + str(
                Date_Time_Arithmetic.Subsec_dateTime_str(str(end_time), 90)) + "' AND ts <= '" + str(
                end_time) + "' AND eid = 'b5ad8b64' AND v <=1550 ORDER BY ts asc"
            print('供氧结束时间前90秒，枪距液面小于1550的值', sql)
            sql_values = client.execute(sql)
            print('小于值的数量，即为多少秒', len(sql_values) / 60)
            smeltv1['desulfurization_lance_time'] = len(sql_values) / 60
            print('压枪时间', smeltv1['desulfurization_lance_time'])



    def cycle_interval(self,start_time, end_time, result):
        '''
        :param start_time: start_time
        :param end_time: end_time
        :param result: result
        :return:
        '''
        if smeltv1[str(start_time)] != None and smeltv1[str(end_time)] != None and smeltv1[
            str(start_time)] != 'None' and smeltv1[str(end_time)] != 'None':
            smeltv1[str(result)] = float(int(str(
                Date_Time_Arithmetic.Endday_sub_Startday_sec(str(smeltv1[str(start_time)]),
                                                             str(smeltv1[str(end_time)])))) / 60)
            # print('cycle_interval:', sql)
        else:
            print('缺少时间')
            smeltv1[str(result)] = 0

    # 加废钢在加废钢之前反推2分钟,即0-2分钟的众数
    def sql_weight_value(self,start_time, end_time, eid, roundnum=1, mode=1):
        sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
              str(start_time) + "' and ts <= '" + str(end_time) + "' AND  eid='" + eid + "';"
        # print('采集炉号SQL', sql)
        sql_values = self.client.execute(sql)
        # print('采集炉号返回值', sql_values)
        values_list = []
        if sql_values:
            if mode == 1:
                for n in range(len(sql_values)):
                    values_list.append(round(sql_values[n][1], roundnum))
                # print(len(values_list) ,type(values_list),values_list)
                # sql_values = statistics.mode(values_list)
                values_list = pd.Series(values_list) * 10
                values_list = values_list.to_list()
                values_list = list(map(int, values_list))
                # print(type(values_list),values_list)
                sql_values = (np.argmax(np.bincount(values_list))) / 10
                # print(type(sql_values), sql_values)
            elif mode == 2:
                for n in range(len(sql_values)):
                    values_list.append(sql_values[n][1])
                print(len(values_list), type(values_list), values_list)
                # sql_values = statistics.mode(values_list)
                print(type(values_list), values_list)
                sql_values = round(np.mean(values_list), roundnum)
                # print(type(sql_values), sql_values)
        else:
            sql_values = '0'
        return sql_values

    # endregion

    # 周期处理
    def cycle_processing(self,start_time, end_time, date_today, i):
        # str格式转换datetime
        temp_start_time_list = datetime.datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
        temp_end_time_list = datetime.datetime.strptime(str(end_time), "%Y-%m-%d %H:%M:%S")

        # 后一日的时间
        temp_date_today = datetime.datetime.strptime(str(date_today + ' 00:00:00'),
                                                     "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=1)
        print('时间比较', temp_start_time_list, temp_date_today)

        # 比较，在开始结束时间数组内，剔除掉超过当日的时间，
        if temp_start_time_list <= temp_date_today:
            smeltv1['smeltPeriod_begin'] = start_time
            smeltv1['smeltPeriod_end'] = end_time
            print(start_time, end_time)

            smeltv1['addScrap_begin'] = str(self.sql_start_time(i, '975a2d9a'))
            print('加废钢开始时间', smeltv1['addScrap_begin'])
            smeltv1['addScrap_end'] =str( self.sql_end_time(i, '975a2d9a'))
            print('加废钢结束时间', smeltv1['addScrap_end'])

            # 加废钢在加废钢之前反推2分钟,即0-2分钟的众数
            if smeltv1['addScrap_begin'] != None and smeltv1['addScrap_begin'] != 'None':
                temp_end_addScrap_begin = Date_Time_Arithmetic.Submin_dateTime_str(smeltv1['addScrap_begin'], 0)
                temp_start_addScrap_begin = Date_Time_Arithmetic.Submin_dateTime_str(smeltv1['addScrap_begin'], 2)
                print('加废钢前0分钟', temp_end_addScrap_begin, '加废钢前2分钟', temp_start_addScrap_begin)

                smeltv1['GrossScrapWeight103'] = str(self.sql_weight_value(temp_start_addScrap_begin, temp_end_addScrap_begin,
                                                                  'ypwnsgz6'))
                print('103号加废钢天车废钢毛重', smeltv1['GrossScrapWeight103'])
                smeltv1['GrossScrapWeight104'] = str(self.sql_weight_value(temp_start_addScrap_begin, temp_end_addScrap_begin,
                                                                  'jnpxj7vp'))
                print('104号加废钢天车废钢毛重', smeltv1['GrossScrapWeight104'])
                # 对铁完之后，2分钟-3分钟之间 取众数
                if smeltv1['addScrap_end'] != None:
                    temp_end_addScrap_end = Date_Time_Arithmetic.Addmin_dateTime_str(smeltv1['addScrap_end'], 1)
                    temp_start_addScrap_end = Date_Time_Arithmetic.Addmin_dateTime_str(smeltv1['addScrap_end'], 0)
                    print('加废钢后2分钟', temp_start_addScrap_end, '加废钢后3分钟', temp_end_addScrap_end, )

                    smeltv1['ScrapTare103'] = str(self.sql_weight_value(temp_start_addScrap_end, temp_end_addScrap_end,
                                                               'ypwnsgz6'))
                    print('103号加废钢天车废钢皮重', smeltv1['ScrapTare103'])
                    smeltv1['ScrapTare104'] = str(self.sql_weight_value(temp_start_addScrap_end, temp_end_addScrap_end,
                                                               'jnpxj7vp'))
                    print('104号加废钢天车废钢皮重', smeltv1['ScrapTare104'])

                    smeltv1['ScrapWeight103'] = str(
                        float(smeltv1['GrossScrapWeight103']) - float(smeltv1['ScrapTare103']))
                    print('103号加废钢天车废钢重量', smeltv1['ScrapWeight103'])
                    smeltv1['ScrapWeight104'] = str(
                        float(smeltv1['GrossScrapWeight104']) - float(smeltv1['ScrapTare104']))
                    print('104号加废钢天车废钢重量', smeltv1['ScrapWeight104'])

            smeltv1['addIron_begin'] = str(self.sql_start_time(i, 'f46bc741'))
            print('兑铁开始时间', smeltv1['addIron_begin'])
            smeltv1['addIron_end'] = str(self.sql_start_time(i, 'f46bc741'))
            print('兑铁结束时间', smeltv1['addIron_end'])

            # 加废钢在加废钢之前反推2分钟,即0-2分钟的众数
            if smeltv1['addIron_begin'] != None and smeltv1['addIron_begin'] != 'None':
                temp_end_addIron_begin = Date_Time_Arithmetic.Submin_dateTime_str(smeltv1['addIron_begin'], 0)
                temp_start_addIron_begin = Date_Time_Arithmetic.Submin_dateTime_str(smeltv1['addIron_begin'], 2)
                print('兑铁前0分钟', temp_end_addIron_begin, '兑铁前2分钟', temp_start_addIron_begin)

                smeltv1['GrossIronWeight101'] = str(self.sql_weight_value(temp_start_addIron_begin, temp_end_addIron_begin,
                                                                 '8sx1j18z'))
                print('101号兑铁天车铁水毛重', smeltv1['GrossIronWeight101'])
                smeltv1['GrossIronWeight102'] = str(self.sql_weight_value(temp_start_addIron_begin, temp_end_addIron_begin,
                                                                 'onevd1bg'))
                print('102号兑铁天车铁水毛重', smeltv1['GrossIronWeight102'])
                # 对铁完之后，2分钟-3分钟之间 取众数
                if smeltv1['addIron_end'] != None:
                    temp_end_addIron_end = Date_Time_Arithmetic.Addmin_dateTime_str(smeltv1['addIron_end'], 1)
                    temp_start_addIron_end = Date_Time_Arithmetic.Addmin_dateTime_str(smeltv1['addIron_end'], 0)
                    print('兑铁后2分钟', temp_start_addIron_end, '兑铁后3分钟', temp_end_addIron_end, )

                    smeltv1['IronTare101'] = str(self.sql_weight_value(temp_start_addIron_end, temp_end_addIron_end,
                                                              '8sx1j18z'))
                    print('101号兑铁天车铁水皮重', smeltv1['IronTare101'])
                    smeltv1['IronTare102'] = str(self.sql_weight_value(temp_start_addIron_end, temp_end_addIron_end,
                                                              'onevd1bg'))
                    print('102号兑铁天车铁水皮重', smeltv1['IronTare102'])

                    smeltv1['IronWeight101'] = str(float(smeltv1['GrossIronWeight101']) - float(smeltv1['IronTare101']))
                    print('101号兑铁天车铁水重量', smeltv1['IronWeight101'])
                    smeltv1['IronWeight102'] = str(float(smeltv1['GrossIronWeight102']) - float(smeltv1['IronTare102']))
                    print('102号兑铁天车铁水重量', smeltv1['IronWeight102'])

            print('加废钢间隔时间 = 兑铁开始时间 - 加废钢结束时间', smeltv1['addIron_begin'], smeltv1['addScrap_end'])
            self.cycle_interval(start_time='addScrap_end', end_time='addIron_begin', result='addScrap_interval')
            print('加废钢间隔时间', smeltv1['addScrap_interval'])

            print('加废钢周期时间 = 兑铁开始时间 - 加废钢开始时间', smeltv1['addIron_begin'], smeltv1['addScrap_begin'])
            self.cycle_interval(start_time='addScrap_begin', end_time='addIron_begin', result='addScrap_cycle')
            print('加废钢周期时间', smeltv1['addScrap_cycle'])

            smeltv1['lowerOxy_begin'] = str(self.sql_start_time(i, 'a8aef6e9'))
            print('吹炼开始时间', smeltv1['lowerOxy_begin'])
            smeltv1['raiseOxy_end'] = str(self.sql_end_time(i, 'a8aef6e9'))
            print('吹炼结束时间', smeltv1['raiseOxy_end'])

            print('兑铁间隔时间 = 吹炼开始时间 - 兑铁结束时间', smeltv1['addIron_end'], smeltv1['lowerOxy_begin'])
            self.cycle_interval(start_time='addIron_end', end_time='lowerOxy_begin', result='addIron_interval')
            print('兑铁间隔时间', smeltv1['addIron_interval'])

            print('兑铁周期时间 = 吹炼开始时间 - 兑铁开始时间', smeltv1['addScrap_begin'], smeltv1['lowerOxy_begin'])
            self.cycle_interval(start_time='addIron_begin', end_time='lowerOxy_begin', result='addIron_cycle')
            print('兑铁周期时间', smeltv1['addIron_cycle'])

            smeltv1['tempMeasur_begin'] = str(self.sql_start_time(i, 'a6948894'))
            print('测温取样开始时间', smeltv1['tempMeasur_begin'])
            smeltv1['tempMeasur_end'] = str(self.sql_start_time(i, 'a6948894'))
            print('测温取样结束时间', smeltv1['tempMeasur_end'])

            print('吹炼间隔时间 = 测温取样开始时间 - 吹炼结束时间', smeltv1['raiseOxy_end'], smeltv1['tempMeasur_begin'])
            self.cycle_interval(start_time='raiseOxy_end', end_time='tempMeasur_begin', result='lowerOxy_interval')
            print('吹炼间隔时间', smeltv1['lowerOxy_interval'])

            print('吹炼周期时间 = 测温取样开始时间 - 吹炼开始时间', smeltv1['lowerOxy_begin'], smeltv1['tempMeasur_begin'])
            self.cycle_interval(start_time='lowerOxy_begin', end_time='tempMeasur_begin', result='lowerOxy_cycle')
            print('吹炼周期时间', smeltv1['lowerOxy_cycle'])

            # if float(smeltv1['lowerOxy_cycle']) < 0.03 and smeltv1['moltenSteel_begin'] != None and smeltv1[
            #     'lowerOxy_begin'] != None:
            #     if smeltv1['moltenSteel_begin'] > smeltv1['lowerOxy_begin']:
            #         print('吹炼周期时间 = 出钢开始时间 - 吹炼开始时间', smeltv1['lowerOxy_begin'],
            #               smeltv1['moltenSteel_begin'])
            #         self.cycle_interval(start_time='moltenSteel_begin', end_time='lowerOxy_begin', result='lowerOxy_cycle')
            #         print('吹炼周期时间', smeltv1['lowerOxy_cycle'], type(smeltv1['lowerOxy_cycle']))

            smeltv1['lowerTwoOxy_begin'] = str(self.sql_start_time(i, 'd814f6f3'))
            print('二吹氧开始时间', smeltv1['lowerTwoOxy_begin'])
            smeltv1['raiseTwoOxy_end'] = str(self.sql_start_time(i, 'd814f6f3'))
            print('二吹氧结束时间', smeltv1['raiseTwoOxy_end'])

            smeltv1['moltenSteel_begin'] = str(self.sql_start_time(i, '75e23142'))
            print('出钢开始时间', smeltv1['moltenSteel_begin'])

            # 煤气回收，查看二吹，有没有，再查一吹。取最后时间，抓起煤气回收。
            converterGasRecoveryTime_min = int(self.sql_set_time_value(str(smeltv1['moltenSteel_begin']), 'ff54d2a6'))
            converterGasRecoveryTime_sec = int(self.sql_set_time_value(str(smeltv1['moltenSteel_begin']), '4e724ef6'))

            smeltv1['converterGasRecoveryTime'] = converterGasRecoveryTime_min + float(
                converterGasRecoveryTime_sec / 60)
            print('煤气回收时间', smeltv1['converterGasRecoveryTime'])

            sublance_temperature = float(self.sql_end_time_value(i, 'c4ed7b6f'))
            smeltv1['sublance_temperature'] = round(sublance_temperature, 2)
            print('转炉钢水副枪测温', smeltv1['sublance_temperature'])

            TSC_Carbon_Content = float(self.sql_end_time_value(i, 'b87cb7f6'))
            smeltv1['TSC_Carbon_Content'] = round(TSC_Carbon_Content, 2)
            print('副枪TSC碳含量百分比', smeltv1['TSC_Carbon_Content'])

            smeltv1['moltenSteel_end'] = self.sql_start_time(i, '75e23142')
            print('出钢结束时间', smeltv1['moltenSteel_end'])

            # 是否有二次吹炼,直接判断测温取样
            if smeltv1['lowerTwoOxy_begin'] == None:
                print('测温取样间隔时间 = 出钢开始时间 - 测温取样结束时间', smeltv1['tempMeasur_end'], smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='tempMeasur_end', end_time='moltenSteel_begin', result='tempMeasur_interval')
                print('测温取样间隔时间', smeltv1['tempMeasur_interval'])

                print('测温取样周期时间 = 出钢开始时间 - 测温取样开始时间', smeltv1['tempMeasur_begin'], smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='tempMeasur_begin', end_time='moltenSteel_begin', result='tempMeasur_cycle')
                print('测温取样周期时间', smeltv1['tempMeasur_cycle'])

                smeltv1['lowerTwoOxy_interval'] = 0
                smeltv1['lowerTwoOxy_cycle'] = 0
            else:
                print('测温取样间隔时间 = 二吹氧开始时间 - 测温取样结束时间', smeltv1['tempMeasur_end'], smeltv1['lowerTwoOxy_begin'])
                self.cycle_interval(start_time='tempMeasur_end', end_time='lowerTwoOxy_begin', result='tempMeasur_interval')
                print('测温取样间隔时间', smeltv1['tempMeasur_interval'])

                print('测温取样周期时间 = 二吹氧开始时间 - 测温取样开始时间', smeltv1['tempMeasur_begin'], smeltv1['lowerTwoOxy_begin'])
                self.cycle_interval(start_time='tempMeasur_begin', end_time='lowerTwoOxy_begin', result='tempMeasur_cycle')
                print('测温取样周期时间', smeltv1['tempMeasur_cycle'])

                print('二吹氧间隔时间 = 出钢开始时间 - 二吹氧结束时间', smeltv1['raiseTwoOxy_end'], smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='raiseTwoOxy_end', end_time='moltenSteel_begin',
                               result='lowerTwoOxy_interval')
                print('二吹氧间隔时间', smeltv1['lowerTwoOxy_interval'])

                print('二吹氧周期时间 = 出钢开始时间 - 二吹氧开始时间', smeltv1['lowerTwoOxy_begin'], smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='lowerTwoOxy_begin', end_time='moltenSteel_begin', result='lowerTwoOxy_cycle')
                print('二吹氧周期时间', smeltv1['lowerTwoOxy_cycle'])

            if float(smeltv1['lowerOxy_cycle']) < 0.03:
                print('吹炼周期时间 = 出钢开始时间 - 吹炼开始时间', smeltv1['lowerOxy_begin'],
                      smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='moltenSteel_begin', end_time='lowerOxy_begin', result='lowerOxy_cycle')
                self.cycle_interval(start_time='lowerOxy_begin', end_time='moltenSteel_begin', result='lowerOxy_cycle')
                print('没有测温取样吹炼周期时间', smeltv1['lowerOxy_cycle'], type(smeltv1['lowerOxy_cycle']))

                print('吹炼间隔时间 = 出钢开始时间 - 吹炼结束时间', smeltv1['raiseOxy_end'],
                      smeltv1['moltenSteel_begin'])
                self.cycle_interval(start_time='raiseOxy_end', end_time='moltenSteel_begin', result='lowerOxy_interval')
                print('吹炼间隔时间', smeltv1['lowerOxy_interval'])


            smeltv1['lowerNitrogen_begin'] = self.sql_start_time(i, 'daf94a70')
            print('溅渣开始时间', smeltv1['lowerNitrogen_begin'])
            smeltv1['raiseNitrogen_end'] = self.sql_end_time(i, 'daf94a70')
            print('溅渣结束时间', smeltv1['raiseNitrogen_end'])

            smeltv1['pourSlag_begin'] = self.sql_start_time(i, '83ae6f89')
            print('倒渣开始时间', smeltv1['pourSlag_begin'])
            smeltv1['pourSlag_end'] = self.sql_end_time(i, '83ae6f89')
            print('倒渣结束时间', smeltv1['pourSlag_end'])

            # 是否有溅渣,直接倒渣
            if smeltv1['lowerNitrogen_begin'] == None:  # 没有溅渣状态位来，即没有溅渣
                print('出钢间隔时间 = 倒渣开始时间 - 出钢结束时间', smeltv1['moltenSteel_end'], smeltv1['pourSlag_begin'])
                self.cycle_interval(start_time='moltenSteel_end', end_time='pourSlag_begin', result='moltenSteel_interval')
                print('出钢间隔时间', smeltv1['moltenSteel_interval'])

                print('出钢周期时间 = 倒渣开始时间 - 出钢开始时间', smeltv1['moltenSteel_begin'], smeltv1['pourSlag_begin'])
                self.cycle_interval(start_time='moltenSteel_begin', end_time='pourSlag_begin', result='moltenSteel_cycle')
                print('出钢周期时间', smeltv1['moltenSteel_cycle'])
                smeltv1['lowerNitrogen_interval'] = 0
                smeltv1['lowerNitrogen_cycle'] = 0
            else:

                print('出钢间隔时间 = 溅渣开始时间 - 出钢结束时间', smeltv1['moltenSteel_end'], smeltv1['lowerNitrogen_begin'])
                self.cycle_interval(start_time='moltenSteel_end', end_time='lowerNitrogen_begin',
                               result='moltenSteel_interval')
                print('出钢间隔时间', smeltv1['moltenSteel_interval'])

                print('出钢周期时间 = 溅渣开始时间 - 出钢开始时间', smeltv1['moltenSteel_begin'], smeltv1['lowerNitrogen_begin'])
                self.cycle_interval(start_time='moltenSteel_begin', end_time='lowerNitrogen_begin',
                               result='moltenSteel_cycle')
                print('出钢周期时间', smeltv1['moltenSteel_cycle'])

                print('溅渣间隔时间 = 倒渣开始时间 - 溅渣结束时间', smeltv1['raiseNitrogen_end'], smeltv1['pourSlag_begin'])
                self.cycle_interval(start_time='raiseNitrogen_end', end_time='pourSlag_begin',
                               result='lowerNitrogen_interval')
                print('溅渣间隔时间', smeltv1['lowerNitrogen_interval'])

                print('溅渣周期时间 = 倒渣开始时间 - 溅渣开始时间', smeltv1['lowerNitrogen_begin'], smeltv1['pourSlag_begin'])
                self.cycle_interval(start_time='lowerNitrogen_begin', end_time='pourSlag_begin',
                               result='lowerNitrogen_cycle')
                print('溅渣周期时间', smeltv1['lowerNitrogen_cycle'])

            # 是否有倒渣结束时间
            if smeltv1['pourSlag_end'] != None:
                print('倒渣间隔时间 = 炉次结束时间 - 倒渣结束时间', smeltv1['pourSlag_end'], smeltv1['smeltPeriod_end'])
                self.cycle_interval(start_time='pourSlag_end', end_time='smeltPeriod_end', result='pourSlag_interval')
                print('倒渣间隔时间', smeltv1['pourSlag_interval'])

                print('倒渣周期时间 = 炉次结束时间 - 倒渣开始时间', smeltv1['pourSlag_begin'], smeltv1['smeltPeriod_end'])
                self.cycle_interval(start_time='pourSlag_begin', end_time='smeltPeriod_end', result='pourSlag_cycle')
                print('倒渣周期时间', smeltv1['pourSlag_cycle'])
            else:
                smeltv1['pourSlag_interval'] = 0
                smeltv1['pourSlag_cycle'] = 0

            smeltv1['supplyOxy_begin'] = self.sql_start_time(i, '0082c988')
            print('供氧状态开始时间', smeltv1['supplyOxy_begin'])
            smeltv1['supplyOxy_end'] = self.sql_end_time(i, '0082c988')
            print('供氧状态结束时间', smeltv1['supplyOxy_end'])

            # 2023.05.10 压枪时间

            if smeltv1['supplyOxy_begin'] != None and smeltv1['supplyOxy_begin'] != 'None' and smeltv1[
                'supplyOxy_end'] != None and smeltv1['supplyOxy_end'] != 'None' and (
                    smeltv1['supplyOxy_end'] - smeltv1['supplyOxy_begin']).seconds >= 600.0:  # 办法有供氧状态开始时间
                # 递归读取压枪时间
                self.desulfurization_lance_time(smeltv1['supplyOxy_begin'], smeltv1['supplyOxy_end'])


            # 供氧状态来，10分钟之后，点吹炼结束（1》0） 时间，前1分钟，找来看枪位最低点，一直持续的时间。 9cbbc927 枪位   0082c988供氧状态
            # 获取供氧状态10分钟之后到供氧结束时间之间的顺序状态值
            # if smeltv1['supplyOxy_begin'] != None and smeltv1['supplyOxy_begin'] != 'None' and smeltv1[
            #     'supplyOxy_end'] != None and smeltv1['supplyOxy_end'] != 'None':  # 办法有供氧状态开始时间
            #     sql = "SELECT ts,v FROM power.collect_steelmaking WHERE ts >= '" + str(
            #         Date_Time_Arithmetic.Addmin_dateTime_str(str(smeltv1['supplyOxy_begin']),
            #                                                  10)) + "' and ts <= '" + str(
            #         smeltv1['supplyOxy_end']) + "' AND  eid='0082c988'  ORDER BY ts asc"
            #
            #     print('压枪时间的sql', sql)
            #     sql_values = self.client.execute(sql)
            #     first_stop_supplyOxy_time = None
            #     if sql_values:
            #         Num_v = 0
            #         # print(sql_values)
            #         for v in sql_values:
            #             Num_v += 1
            #             if int(v[1]) == 0:
            #                 first_stop_supplyOxy_time = v[0]
            #                 print('判断供氧状态中有0，获取第一次停氧时间', first_stop_supplyOxy_time)
            #                 break
            #         else:
            #             first_stop_supplyOxy_time = smeltv1['supplyOxy_end']
            #             print('判断供氧状态中没有0，获取第一次停氧时间', first_stop_supplyOxy_time)
            #
            #         sql = "SELECT MIN(v), MIN(ts) AS start_time, MAX(ts) AS end_time FROM power.collect_steelmaking WHERE ts >= '" + str(
            #             Date_Time_Arithmetic.Submin_dateTime_str(str(first_stop_supplyOxy_time),
            #                                                      1)) + "' AND ts <= '" + str(
            #             first_stop_supplyOxy_time) + "' AND eid='9cbbc927' AND v = (SELECT MIN(v) FROM power.collect_steelmaking WHERE ts >= '" + str(
            #             Date_Time_Arithmetic.Submin_dateTime_str(str(first_stop_supplyOxy_time),
            #                                                      1)) + "' AND ts <= '" + str(
            #             first_stop_supplyOxy_time) + "' AND eid='9cbbc927')"
            #         print(
            #             '这条查询语句会返回最小值、最小值开始时间和最小值结束时间三个结果。其中，最小值是通过子查询找到的，而最小值开始时间和结束时间则是在主查询中通过 MIN 和 MAX 聚合函数计算得出的',
            #             sql)
            #         sql_values = self.client.execute(sql)
            #         if sql_values:
            #             print('两个时间', sql_values)
            #             smeltv1['desulfurization_lance_min_position'] = float(sql_values[0][0])
            #             smeltv1['desulfurization_lance_begin'] = str(sql_values[0][1])
            #             smeltv1['desulfurization_lance_end'] = str(sql_values[0][2])
            #             smeltv1['desulfurization_lance_time'] = (sql_values[0][2] - sql_values[0][
            #                 1]).total_seconds() / 60
            #
            #         else:
            #             smeltv1['desulfurization_lance_time'] = '0'
            #
            #     else:
            #         smeltv1['desulfurization_lance_time'] = '0'
            # else:
            #     smeltv1['desulfurization_lance_time'] = '0'





            print('压枪时间', smeltv1['desulfurization_lance_time'])
            print('压枪时间枪位最小值', smeltv1['desulfurization_lance_min_position'])
            print('压枪时间枪位最小值开始时间', smeltv1['desulfurization_lance_begin'])
            print('压枪时间枪位最小值结束时间', smeltv1['desulfurization_lance_end'])

            smeltv1['splashSlag_begin'] = self.sql_start_time(i, '5448d76f')
            print('供氮状态开始时间', smeltv1['splashSlag_begin'])
            smeltv1['splashSlag_out'] = self.sql_end_time(i, '5448d76f')
            print('供氮状态结束时间', smeltv1['splashSlag_out'])

            smeltv1['heatNo'] = int(self.sql_end_time_value(i, '360332d1'))
            print('结束时间的炉号', smeltv1['heatNo'])

            smeltv1['heatAge'] = int(self.sql_end_time_value(i, '6151241d'))
            print('结束时间的炉龄', smeltv1['heatAge'])

            addScrap_min = int(self.sql_end_time_value(i, 'ec299bb7'))
            addScrap_sec = int(self.sql_end_time_value(i, '03eb4301'))
            smeltv1['addScrap'] = addScrap_min + float(addScrap_sec / 60)
            print('加废钢时间', smeltv1['addScrap'])

            addIron_min = int(self.sql_end_time_value(i, 'b14886aa'))
            addIron_sec = int(self.sql_end_time_value(i, 'c2d8fa80'))
            smeltv1['addIron'] = addIron_min + float(addIron_sec / 60)
            print('兑铁时间', smeltv1['addIron'])

            supplyOxy_min = int(self.sql_end_time_value(i, 'e11b1c10'))
            supplyOxy_sec = int(self.sql_end_time_value(i, 'ccb0f3f5'))
            smeltv1['supplyOxy'] = supplyOxy_min + float(supplyOxy_sec / 60)
            print('吹炼时间', smeltv1['supplyOxy'])

            # 双渣 如果吹炼时间大于6分钟，判断从3分钟 到6分钟之间 是否有 供氧状态为0 并且 倾动角度大于 70
            print('吹炼开始时间', smeltv1['lowerOxy_begin'])

            if float(smeltv1['supplyOxy']) is not None and float(smeltv1['supplyOxy']) >= 6.0 and smeltv1['lowerOxy_begin'] != 'None' and smeltv1['lowerOxy_begin'] != None:
                print('周期的开始时间和结束时间', start_time, end_time)
                # 吹炼开始时间的 3分钟和6分钟
                temp_start_time_supplyOxy = Date_Time_Arithmetic.Addsec_dateTime_str(smeltv1['lowerOxy_begin'], 180)
                temp_end_time_supplyOxy = Date_Time_Arithmetic.Addsec_dateTime_str(smeltv1['lowerOxy_begin'], 360)
                set_convert_angle = 70.0
                print('吹炼开始时间3分钟', temp_start_time_supplyOxy)
                print('吹炼开始时间6分钟', temp_end_time_supplyOxy)

                # 在吹炼的3-6分钟内，查看供氧状态是否有为0
                sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + temp_start_time_supplyOxy + "' and ts <= '" + temp_end_time_supplyOxy + "' AND  eid='0082c988' and v = 0  ORDER BY ts desc LIMIT 1;"
                sql_values = self.client.execute(sql)
                if sql_values:
                    print('在吹炼的1-6分钟内，查看供氧状态是否有为0,有停止供氧')
                    sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + temp_start_time_supplyOxy + "' and ts <= '" + temp_end_time_supplyOxy + "' AND eid='6d9c9514' and v >= '" + str(
                        set_convert_angle) + "'   ORDER BY ts asc LIMIT 1;"
                    sql_values = self.client.execute(sql)
                    if sql_values:
                        print('在吹炼的1-6分钟内，停止供氧，转炉旋转大于70°,定为双渣')
                        smeltv1['double_slag_state'] = 1
                    else:
                        smeltv1['double_slag_state'] = 0
                else:
                    print('在吹炼的3-6分钟内，未停止供氧')
                    smeltv1['double_slag_state'] = 0


            supplyTwoOxy_min = int(self.sql_end_time_value(i, '55f52029'))
            supplyTwoOxy_sec = int(self.sql_end_time_value(i, 'dcda1666'))
            smeltv1['supplyTwoOxy'] = supplyTwoOxy_min + float(supplyTwoOxy_sec / 60)
            print('二吹氧时间', smeltv1['supplyTwoOxy'])

            tempMeasure_min = int(self.sql_end_time_value(i, '7f4f89fa'))
            tempMeasure_sec = int(self.sql_end_time_value(i, 'e25967ed'))
            smeltv1['tempMeasure'] = tempMeasure_min + float(tempMeasure_sec / 60)
            print('测温时间', smeltv1['tempMeasure'])

            beforSteelTemp1 = float(self.sql_end_time_value(i, 'af2d569c'))
            beforSteelTemp2 = float(self.sql_end_time_temperature_after_argon(i, 'cf0d672c'))
            smeltv1['beforSteelTemp1'] = round(beforSteelTemp1, 2)
            smeltv1['beforSteelTemp2'] = round(beforSteelTemp2, 2)
            print('转炉钢水自动测温1', smeltv1['beforSteelTemp1'])
            print('转炉钢水自动测温2', smeltv1['beforSteelTemp2'])

            moltenSteel_min = int(self.sql_end_time_value(i, 'd4415fa9'))
            moltenSteel_sec = int(self.sql_end_time_value(i, 'c7e089dd'))
            smeltv1['moltenSteel'] = moltenSteel_min + float(moltenSteel_sec / 60)
            print('出钢时间', smeltv1['moltenSteel'])

            splashSlag_min = int(self.sql_end_time_value(i, '474028c6'))
            splashSlag_sec = int(self.sql_end_time_value(i, '7d658c82'))
            smeltv1['splashSlag'] = splashSlag_min + float(splashSlag_sec / 60)
            print('溅渣时间', smeltv1['splashSlag'])

            pourSlag_min = int(self.sql_end_time_value(i, '1ddce1a6'))
            pourSlag_sec = int(self.sql_end_time_value(i, '320b7ebf'))
            smeltv1['pourSlag'] = pourSlag_min + float(pourSlag_sec / 60)
            print('倒渣时间', smeltv1['pourSlag'])

            controlOxygenTime_min = int(self.sql_end_time_value(i, '01c531f1'))
            controlOxygenTime_sec = int(self.sql_end_time_value(i, 'dec4e4fd'))
            smeltv1['controlOxygenTime'] = controlOxygenTime_min + float(controlOxygenTime_sec / 60)
            print('供氧时间', smeltv1['controlOxygenTime'])

            controlNitrogenTime_min = int(self.sql_end_time_value(i, '4b6d5fce'))
            controlNitrogenTime_sec = int(self.sql_end_time_value(i, '915a794d'))
            smeltv1['controlNitrogenTime'] = controlNitrogenTime_min + float(controlNitrogenTime_sec / 60)
            print('供氮时间', smeltv1['controlNitrogenTime'])

            hopperWeightHigh1Day = int(self.sql_end_time_value(i, 'a5e462a9'))
            smeltv1['hopperWeightHigh1Day'] = hopperWeightHigh1Day
            print('120吨1号转炉高位1#仓单炉下料量', smeltv1['hopperWeightHigh1Day'])

            hopperWeightHigh2Day = int(self.sql_end_time_value(i, '9c6e0cff'))
            smeltv1['hopperWeightHigh2Day'] = hopperWeightHigh2Day
            print('120吨1号转炉高位2#仓单炉下料量', smeltv1['hopperWeightHigh2Day'])

            hopperWeightHigh3Day = int(self.sql_end_time_value(i, '54e651c7'))
            smeltv1['hopperWeightHigh3Day'] = hopperWeightHigh3Day
            print('120吨1号转炉高位3#仓单炉下料量', smeltv1['hopperWeightHigh3Day'])

            # --------------
            hopperWeightHigh4Day = int(self.sql_end_time_value(i, 'c7229ade'))
            smeltv1['hopperWeightHigh4Day'] = hopperWeightHigh4Day
            print('120吨1号转炉高位4#仓单炉下料量', smeltv1['hopperWeightHigh4Day'])

            hopperWeightHigh5Day = int(self.sql_end_time_value(i, '6a6c36c3'))
            smeltv1['hopperWeightHigh5Day'] = hopperWeightHigh5Day
            print('120吨1号转炉高位5#仓单炉下料量', smeltv1['hopperWeightHigh5Day'])

            hopperWeightHigh6Day = int(self.sql_end_time_value(i, 'b3bfbf15'))
            smeltv1['hopperWeightHigh6Day'] = hopperWeightHigh6Day
            print('120吨1号转炉高位6#仓单炉下料量', smeltv1['hopperWeightHigh6Day'])

            hopperWeightHigh7Day = int(self.sql_end_time_value(i, '842db5c8'))
            smeltv1['hopperWeightHigh7Day'] = hopperWeightHigh7Day
            print('120吨1号转炉高位7#仓单炉下料量', smeltv1['hopperWeightHigh7Day'])

            hopperWeightHigh8Day = int(self.sql_end_time_value(i, '1daac907'))
            smeltv1['hopperWeightHigh8Day'] = hopperWeightHigh8Day
            print('120吨1号转炉高位8#仓单炉下料量', smeltv1['hopperWeightHigh8Day'])

            hopperWeightHigh9Day = int(self.sql_end_time_value(i, '9d3125a6'))
            smeltv1['hopperWeightHigh9Day'] = hopperWeightHigh9Day
            print('120吨1号转炉高位9#仓单炉下料量', smeltv1['hopperWeightHigh9Day'])

            hopperWeightHigh10Day = int(self.sql_end_time_value(i, 'c5e7e057'))
            smeltv1['hopperWeightHigh10Day'] = hopperWeightHigh10Day
            print('120吨1号转炉高位10#仓单炉下料量', smeltv1['hopperWeightHigh10Day'])

            hopperWeightMiddle1Day = int(self.sql_end_time_value(i, 'f4b890a9'))
            smeltv1['hopperWeightMiddle1Day'] = hopperWeightMiddle1Day
            print('120吨1号转炉中位1#仓单炉下料量', smeltv1['hopperWeightMiddle1Day'])

            hopperWeightMiddle2Day = int(self.sql_end_time_value(i, '67246f30'))
            smeltv1['hopperWeightMiddle2Day'] = hopperWeightMiddle2Day
            print('120吨1号转炉中位2#仓单炉下料量', smeltv1['hopperWeightMiddle2Day'])

            hopperWeightMiddle3Day = int(self.sql_end_time_value(i, 'b0252882'))
            smeltv1['hopperWeightMiddle3Day'] = hopperWeightMiddle3Day
            print('120吨1号转炉中位3#仓单炉下料量', smeltv1['hopperWeightMiddle3Day'])

            hopperWeightMiddle4Day = int(self.sql_end_time_value(i, '91eeb8a1'))
            smeltv1['hopperWeightMiddle4Day'] = hopperWeightMiddle4Day
            print('120吨1号转炉中位4#仓单炉下料量', smeltv1['hopperWeightMiddle4Day'])

            hopperWeightMiddle5Day = int(self.sql_end_time_value(i, 'a3d2bd5d'))
            smeltv1['hopperWeightMiddle5Day'] = hopperWeightMiddle5Day
            print('120吨1号转炉中位5#仓单炉下料量', smeltv1['hopperWeightMiddle5Day'])

            hopperWeightMiddle6Day = int(self.sql_end_time_value(i, '9e7cd22d'))
            smeltv1['hopperWeightMiddle6Day'] = hopperWeightMiddle6Day
            print('120吨1号转炉中位6#仓单炉下料量', smeltv1['hopperWeightMiddle6Day'])

            hopperWeightMiddle7Day = int(self.sql_end_time_value(i, 'ec1ad43a'))
            smeltv1['hopperWeightMiddle7Day'] = hopperWeightMiddle7Day
            print('120吨1号转炉中位7#仓单炉下料量', smeltv1['hopperWeightMiddle7Day'])

            hopperWeightMiddle8Day = int(self.sql_end_time_value(i, '62e0a32d'))
            smeltv1['hopperWeightMiddle8Day'] = hopperWeightMiddle8Day
            print('120吨1号转炉中位8#仓单炉下料量', smeltv1['hopperWeightMiddle8Day'])

            alloyMiddle1 = int(self.sql_end_time_value(i, '29a7556f'))
            smeltv1['alloyMiddle1'] = alloyMiddle1
            print('120吨1号转炉1#铁合金料单中位1#仓', smeltv1['alloyMiddle1'])

            alloyMiddle2 = int(self.sql_end_time_value(i, 'fbc46360'))
            smeltv1['alloyMiddle2'] = alloyMiddle2
            print('120吨1号转炉1#铁合金料单中位2#仓', smeltv1['alloyMiddle2'])

            alloyMiddle3 = int(self.sql_end_time_value(i, '8f952b4a'))
            smeltv1['alloyMiddle3'] = alloyMiddle3
            print('120吨1号转炉1#铁合金料单中位3#仓', smeltv1['alloyMiddle3'])

            alloyMiddle4 = int(self.sql_end_time_value(i, 'f8187957'))
            smeltv1['alloyMiddle4'] = alloyMiddle4
            print('120吨1号转炉1#铁合金料单中位4#仓', smeltv1['alloyMiddle4'])

            alloyMiddle5 = int(self.sql_end_time_value(i, 'b409e62a'))
            smeltv1['alloyMiddle5'] = alloyMiddle5
            print('120吨1号转炉1#铁合金料单中位5#仓', smeltv1['alloyMiddle5'])

            alloyMiddle6 = int(self.sql_end_time_value(i, 'ac17f133'))
            smeltv1['alloyMiddle6'] = alloyMiddle6
            print('120吨1号转炉1#铁合金料单中位6#仓', smeltv1['alloyMiddle6'])

            alloyMiddle7 = int(self.sql_end_time_value(i, '4ccaa9e8'))
            smeltv1['alloyMiddle7'] = alloyMiddle7
            print('120吨1号转炉1#铁合金料单中位7#仓', smeltv1['alloyMiddle7'])

            alloyMiddle8 = int(self.sql_end_time_value(i, 'ab9492da'))
            smeltv1['alloyMiddle8'] = alloyMiddle8
            print('120吨1号转炉1#铁合金料单中位8#仓', smeltv1['alloyMiddle8'])

            alloyHigh1 = int(self.sql_end_time_value(i, '69226b00'))
            smeltv1['alloyHigh1'] = alloyHigh1
            print('120吨1号转炉1#铁合金料单高位1#仓', smeltv1['alloyHigh1'])

            alloyHigh2 = int(self.sql_end_time_value(i, 'aac75ad4'))
            smeltv1['alloyHigh2'] = alloyHigh2
            print('120吨1号转炉1#铁合金料单高位2#仓', smeltv1['alloyHigh2'])

            material1 = int(self.sql_end_time_value(i, 'eed77a8b'))
            smeltv1['material1'] = material1
            print('120吨1号转炉辅料1号仓', smeltv1['material1'])

            material2 = int(self.sql_end_time_value(i, '9a98ba42'))
            smeltv1['material2'] = material2
            print('120吨1号转炉辅料2号仓', smeltv1['material2'])

            material3 = int(self.sql_end_time_value(i, '29ab6694'))
            smeltv1['material3'] = material3
            print('120吨1号转炉辅料3号仓', smeltv1['material3'])

            material4 = int(self.sql_end_time_value(i, '7494ad94'))
            smeltv1['material4'] = material4
            print('120吨1号转炉辅料4号仓', smeltv1['material4'])

            material5 = int(self.sql_end_time_value(i, '16fa8b37'))
            smeltv1['material5'] = material5
            print('120吨1号转炉辅料5号仓', smeltv1['material5'])

            material6 = int(self.sql_end_time_value(i, '7c44957a'))
            smeltv1['material6'] = material6
            print('120吨1号转炉辅料6号仓', smeltv1['material6'])

            material7 = int(self.sql_end_time_value(i, '3eb716f7'))
            smeltv1['material7'] = material7
            print('120吨1号转炉辅料7号仓', smeltv1['material7'])

            material8 = int(self.sql_end_time_value(i, 'ee487051'))
            smeltv1['material8'] = material8
            print('120吨1号转炉辅料8号仓', smeltv1['material8'])

            class_temp = int(self.sql_end_time_value(i, 'b7bacbad'))
            smeltv1['Class'] = class_temp
            print('120吨1号转炉班组', smeltv1['Class'])

            team_temp = int(self.sql_end_time_value(i, '3c51de07'))
            smeltv1['Team'] = team_temp
            print('120吨1号转炉班别', smeltv1['Team'])

            repair_converter_temp = int(self.sql_end_time_value(i, '52fbbb5a'))
            smeltv1['repair_converter'] = repair_converter_temp
            print('120吨2号转炉补炉', smeltv1['repair_converter'])

            smeltv1['steelType'] = int(self.sql_set_time_value(smeltv1['moltenSteel_end'], '9b4b5052'))
            print('钢种信息', smeltv1['steelType'])
            smeltv1['steelSpecs'] = int(self.sql_set_time_value(smeltv1['moltenSteel_end'], '8556791f'))
            print('规格信息', smeltv1['steelSpecs'])
            smeltv1['ironLoading'] = float(self.sql_set_time_value(smeltv1['moltenSteel_end'], 'f83fdc3e'))
            print('铁水装入量', smeltv1['ironLoading'])
            smeltv1['steelLoading'] = float(self.sql_set_time_value(smeltv1['moltenSteel_end'], '9a86d5f0'))
            print('钢水装入量', smeltv1['steelLoading'])

            lanceAge = int(self.sql_end_time_value(i, 'c786f031'))
            smeltv1['lanceAge'] = lanceAge
            print('120吨1号转炉枪龄', smeltv1['lanceAge'])

            smeltPeriod_OxygenWastage = int(self.sql_end_time_value(i, 'ec7f3f6f'))
            smeltv1['smeltPeriod_OxygenWastage'] = smeltPeriod_OxygenWastage
            print('本炉氧气消耗', smeltv1['smeltPeriod_OxygenWastage'])

            smeltPeriod_NitrogenWastage = int(self.sql_end_time_value(i, '6b12f4c8'))
            smeltv1['smeltPeriod_NitrogenWastage'] = smeltPeriod_NitrogenWastage
            print('本炉氮气消耗', smeltv1['smeltPeriod_NitrogenWastage'])

            smeltv1['OxylanceMainPressure'] = self.sql_weight_value(start_time=start_time, end_time=end_time, eid='53f47c0e',
                                                               roundnum=2, mode=2)
            print('氧枪氧气总管平均压力', smeltv1['OxylanceMainPressure'])

            smeltv1['OxylanceBranchMainPressure'] = self.sql_weight_value(start_time=start_time, end_time=end_time,
                                                                          eid='83b59eac', roundnum=2, mode=2)
            print('氧枪氧气总支管平均压力', smeltv1['OxylanceBranchMainPressure'])



            # sql = "REPLACE INTO smeltinfo (smeltinfo.inDate,smeltinfo.heatNo) VALUES('" + smeltv1['inDate'] + "','" + str(smeltv1['heatNo']) + "')"

    def wirte_sql(self):
        print('判断是否写入数据库')
        if smeltv1['inDate'] == str(smeltv1['smeltPeriod_begin'][:10]) and smeltv1['heatNo'] != 0:
            sql_str = ''
            sql_value = ''
            for i in smeltv1.keys():
                # print(i)
                sql_str = sql_str + i + ","
            for i in smeltv1.values():
                # print(i)
                sql_value = sql_value + "'" + str(i) + "',"
            sql = "replace into smeltinfo1 (" + sql_str[:-1] + ")value(" + sql_value[:-1] + ")"

            print(sql)

            self.cur.execute(sql)
            self.conn.commit()
            print('写入冶炼周期写入82数据库')

    # 主程序
    # while flag:
    def main(self):
        global start_time_list
        global end_time_list
        global STOP_start_time_list
        global STOP_end_time_list
        global temp_everday_first_cycle_start_time
        global everday_first_stat
        global process_state
        global smeltv1
        global everday_first_cycle_start_time
        for day in range(self.days):
            self.date_today = datetime.date.today().strftime("%Y-%m-%d")  # 目前日期 str
            # self.date_today = '2023-05-01'  # 设定执行日期
            sub_day = 0 - self.days
            self.date_today = Date_Time_Arithmetic.Add_strday_str(self.date_today, sub_day)  # 循环每次增加一日

            # try:
            # region 设定时间
            # self.date_today = '2022-06-19'  # 设定执行日期
            # self.date_today = Date_Time_Arithmetic.Add_strday_str(self.date_today , day)  # 循环每次增加一日
            self.time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 目前时间 str

            print('今日日期:', self.date_today)
            print('现在时间:', self.time_now, type(self.time_now))
            # endregion

            # region 获取第一笔数据过程
            # 读取当日最一笔转炉未开始状态的时间,炉次状态 = 0 的第一笔数据
            sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + self.date_today + "'  AND (eid='129756f8') and  v =0 ORDER BY ts  asc LIMIT 1 ;"
            print('当日最一笔转炉未开始状态的时间,炉次状态 = 0 的第一笔数据', sql)
            everday_earliest_cycle_STOP_time = self.client.execute(sql)
            print('当日最一笔转炉未开始状态的时间', everday_earliest_cycle_STOP_time)

            # 读取当日最一笔转炉开始状态的时间,炉次状态 = 1 的第一笔数据
            sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + self.date_today + "'  AND (eid='129756f8') and  v =1 ORDER BY ts  asc LIMIT 1 ;"
            print('当日最一笔转炉开始状态的时间,炉次状态 = 1 的第一笔数据', sql)
            everday_earliest_cycle_RUN_time = self.client.execute(sql)
            print('当日最一笔转炉开始状态的时间', everday_earliest_cycle_RUN_time)

            # 获取两个第一笔数据的时候，进行判断出第一炉的开始时间
            if everday_earliest_cycle_STOP_time and everday_earliest_cycle_RUN_time:
                print('当日最一笔转炉未开始与开始状态的时间正常')
                print('当日最一笔转炉开始状态的时间', everday_earliest_cycle_RUN_time[0][0])
                print('当日最一笔转炉未开始状态的时间', everday_earliest_cycle_STOP_time[0][0])

                if everday_earliest_cycle_RUN_time[0][0] > everday_earliest_cycle_STOP_time[0][0]:
                    print('RUN比STOP晚,即0->1,此炉为今日第一炉')

                    # 判断每日第一炉开始时间：
                    everday_first_cycle_start_time = everday_earliest_cycle_RUN_time[0][0]
                    print('每日第一炉开始时间:', everday_first_cycle_start_time, type(everday_first_cycle_start_time))

                    # 过程位赋值
                    everday_first_stat = 1
                    process_state = 10

                elif everday_earliest_cycle_RUN_time[0][0] < everday_earliest_cycle_STOP_time[0][0]:
                    print('RUN比STOP早,即1->0,下一炉为今日第一炉')

                    # 判断前一日，最后一炉结束时间：
                    everday_earliest_cycle_STOP_time = everday_earliest_cycle_STOP_time[0][0]
                    print('判断前一日，最后一炉结束时间:', everday_earliest_cycle_STOP_time)

                    # 过12点,上一炉未结束,待上一日炉次结束 1->0 ,再查找 0->1的第一笔时间
                    sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(everday_earliest_cycle_STOP_time) + "'  AND (eid='129756f8') and  v =1 ORDER BY ts  asc LIMIT 1 ;"
                    print('过12点,上一炉未结束,待上一日炉次结束 1->0 ,再查找 0->1的第一笔时间,', sql)
                    everday_first_cycle_start_time = self.client.execute(sql)
                    everday_first_stat = 2

                    # 搜索第一笔炉次开始时间
                    if everday_first_cycle_start_time:
                        everday_first_cycle_start_time = everday_first_cycle_start_time[0][0]
                        print('每日第一炉开始时间:', everday_first_cycle_start_time, type(everday_first_cycle_start_time))
                        # 过程位赋值
                        everday_first_stat = 3
                        process_state = 10
                    else:
                        print('返回RUN比STOP早,未搜索到,前一日炉次结束，当日第一炉未开始')
                        everday_first_stat = 4
                        process_state = 510
                else:
                    print('RUN与STOP时间相同，不可能，绝对不可能')
                    process_state = 65535

            elif everday_earliest_cycle_STOP_time and len(everday_earliest_cycle_RUN_time) == 0:
                print('当日最一笔转炉未开始状态,炉次状态 = 0->1 的第一笔数据未出现,今日第一炉未开始')
                everday_first_stat = 4
                process_state = 510

            elif everday_earliest_cycle_RUN_time and len(everday_earliest_cycle_STOP_time) == 0:
                print('当日最一笔转炉开始状态,炉次状态 = 1->0 的第一笔数据未出现,正在昨日最后一炉生产中')
                everday_first_stat = 5
                process_state = 500
                # ??
                # everday_first_cycle_start_time = str(everday_earliest_cycle_RUN_time[0][0])
            else:
                print('当日最一笔转炉未开始或开始状态的时间读取为空,选择日期或EID不对')
                process_state = 65535
            # endregion

            # region 校验当日第一次炉次开始时间，是否在当日。
            if everday_first_cycle_start_time:
                # print(everday_first_cycle_start_time[:10])
                everday_first_cycle_date = datetime.datetime.strptime(str(everday_first_cycle_start_time)[:10], "%Y-%m-%d")
                date_today_temp = datetime.datetime.strptime(self.date_today, "%Y-%m-%d")
                print('第一笔的日期', everday_first_cycle_start_time, everday_first_cycle_date,
                      type(everday_first_cycle_date),
                      date_today_temp, type(date_today_temp))
                if everday_first_cycle_date > date_today_temp:
                    print('当日第一笔炉次时间不是当日')
                    everday_first_stat = 6
                    process_state = 65535
                else:
                    temp_everday_first_cycle_start_time = everday_first_cycle_start_time
            # endregion

            # region 获取当日第一笔炉次开始时间状态分类处理:

            if everday_first_stat == 2:  # 得到过程值，重新循环
                print('得到everday_first_stat过程值，重新循环', everday_first_stat)
                process_state = 65535
            elif everday_first_stat == 4:
                print('前日炉次结束,今日炉次未开始', everday_first_stat)
                process_state = 510
                process_state = 65535
            elif everday_first_stat == 5:
                process_state = 500
                print('正在昨日最后一炉生产中', everday_first_stat)
            elif everday_first_stat == 3 or everday_first_stat == 1:
                print('正常进行', process_state)
                process_state = 10
            else:
                print('当日第一笔时间数据异常')
                process_state = 65535
            # endregion

            # 获取当日第一炉时间后，处理后获取时间段
            if process_state == 10:  # 获取当日第一炉次时间
                print('第一阶段:处理第一炉开始时间')
                # 搜索 第一炉开始时间后的所有数据
                sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(everday_first_cycle_start_time) + "' and ts < '" + Date_Time_Arithmetic.Addday_dateTime_str(str(everday_first_cycle_start_time), 1)[:10] + " 00:00:00' AND  eid='" + str(
                    '129756f8') + "'  ORDER BY ts asc ;"
                print('搜索 第一炉开始时间后的 状态 = 1 的今日所有数据', sql)
                sql_values = self.client.execute(sql)
                start_state_temp = 0  # 等废钢临时比较状态

                if sql_values:  # 判断有数据获取
                    # 筛选出大于等于1的值的第一次
                    print('比较筛选次数', len(sql_values))
                    for i in range(0, len(sql_values)):
                        # 判断结果大于0的值和上一周的值是否一致,过滤功能
                        if sql_values[i][1] > start_state_temp:
                            # 将大于0的值 赋值这一周期,即0 -> 1
                            start_state_temp = 1
                            # 保存下来 0 -> 1 的时间存入数组
                            start_time_list.append(sql_values[i][0])
                            # print("当日炉次开始时间",sql_values[i][0])
                        elif sql_values[i][1] == start_state_temp:
                            continue
                        elif sql_values[i][1] == 0 and start_state_temp == 1:
                            # 第一炉次开始时间后数据，停炉开始时间 1 -> 0 的时间存入数组
                            STOP_start_time_list.append(sql_values[i][0])
                            start_state_temp = 0
                        elif sql_values[i][1] == 0 and start_state_temp == 0:
                            start_state_temp = 0
                        else:
                            print(sql_values[i][1], start_state_temp)
                            process_state = 65535
                            print("异常错误")
                    process_state = 20
                else:
                    process_state = 11
                    print('无获取到开始时间数据')

                print('开始时间数量是:', len(start_time_list), start_time_list)
                print('停炉的开始时间数量是', len(STOP_start_time_list), STOP_start_time_list)

                # 停炉的结束时间

            # 获取炉次的结束时间
            if process_state == 20:
                print('第二阶段:处理第一炉结束时间')
                if len(start_time_list) == len(STOP_start_time_list) + 1:
                    print('炉次开始时间数量大于炉次结束时间数量，证明炉次最后没有完成')
                    # 当日最后一炉开始时间后 第一次状态 =0  的时间 ，没有即为正在进行中，用当前时间
                    sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(
                        start_time_list[-1]) + "' AND  eid='129756f8' and v = 0  ORDER BY ts asc LIMIT 1;"
                    sql_values = self.client.execute(sql)
                    if sql_values:
                        STOP_start_time_list.append(sql_values[0][0])
                    else:
                        STOP_start_time_list.append(self.time_now)

                elif len(start_time_list) == len(STOP_start_time_list):
                    print('炉次开始时间数量等于炉次结束时间数量，证明炉次全部完成')
                # print('跨天停炉的开始时间数量是', len(STOP_start_time_list), STOP_start_time_list)

                # else:
                #     print('炉次开始时间数量小于炉次结束时间数量,证明数据异常')

                # 炉次结束时间
                for i in range(len(STOP_start_time_list)):
                    # 搜索大于等于1的点和时间
                    sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(
                        str(start_time_list[i])) + "' and ts < '" + str(
                        STOP_start_time_list[i]) + "' AND  eid='" + str(
                        '129756f8') + "' and v = 1  ORDER BY ts desc LIMIT 1;"
                    # print('炉次的结束时间:',sql)
                    sql_values = self.client.execute(sql)
                    if sql_values:
                        end_time_list.append(sql_values[0][0])
                        # print('读取SQL的数据',sql_values)
                    else:
                        end_time_list.append(self.time_now)  # 没有结束时间，即为现在

                # 停炉的结束时间
                for i in range(len(STOP_start_time_list)):
                    # 搜索大于等于1的点和时间
                    sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(
                        STOP_start_time_list[i]) + "' AND  eid='d3ec720c' and v = 0 ORDER BY ts asc LIMIT 1;"
                    # print('停炉的结束时间:',sql)
                    sql_values = self.client.execute(sql)
                    if sql_values:
                        sql_values = Date_Time_Arithmetic.Subsec_dateTime_str(str(sql_values[0][0]), 1)
                        STOP_end_time_list.append(sql_values)
                        # print('读取SQL的数据',sql_values)
                    else:
                        STOP_end_time_list.append(self.time_now)  # 没有结束时间，即为现在
                        print('')

                # print('开始时间数量是:', len(start_time_list), start_time_list)
                # print('结束时间数量是:', len(end_time_list), end_time_list)
                # print('停炉的开始时间数量是', len(STOP_start_time_list), STOP_start_time_list)
                # print('停炉的结束时间数量是', len(STOP_end_time_list), STOP_end_time_list)
                process_state = 31
                if len(start_time_list) == len(end_time_list):
                    print('正常运行~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    process_state = 30
                else:
                    print('异常~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    process_state = 30

            # 处理炉次信息
            if process_state == 30:
                print('第三阶段:处理炉次信息')
                # 循环判断各炉次时间
                for i in range(len(end_time_list)):
                    print('结束循环次数', i)
                    print('炉次开始时间:', str(start_time_list[i]), type(str(start_time_list[i])))
                    print('炉次结束时间:', str(start_time_list[i]), type(str(start_time_list[i])))

                    # 炉次周期内读取数据
                    self.cycle_processing(start_time=str(start_time_list[i]), end_time=str(end_time_list[i]),
                                     date_today=self.date_today, i=i)

                    # region 转炉全周期
                    cycle_start_time = str(start_time_list[i])  # 炉次开始时间
                    cycle_end_time = str(STOP_start_time_list[i])  # 停炉结束时间

                    print('开始时间到下次开始时间', cycle_start_time, cycle_end_time)

                    smeltv1['inDate'] = self.date_today
                    # 做两个炉次的时间差
                    cycle_time = Date_Time_Arithmetic.Endday_sub_Startday_sec(str(cycle_start_time),
                                                                              str(cycle_end_time))
                    # cycle_time = cycle_end_time - cycle_start_time
                    print('全周期时间:', cycle_time)

                    # 判断是否大于1天处理。
                    # if int(cycle_time) >= 86400:
                    #     cycle_time = (int(cycle_time.days) * 86400) + (int(cycle_time.microseconds) * 60) + int(
                    #         cycle_time.seconds)
                    print('全周期时间秒级:', cycle_time)
                    cycle_min = float(cycle_time / 60)
                    print('全周期时间分钟级:', cycle_min)
                    smeltv1['cycle'] = cycle_min
                    # print('全周期时间分钟级:', cycle)

                    # endregion

                    # 煤气回收，查看二吹，有没有，再查一吹。取最后时间，抓起煤气回收。
                    # 二吹状态 d814f6f3 一吹状态 a8aef6e9
                    # converterGasRecoveryTime_min = int(self.sql_gasrecovery_time(i, 'a8aef6e9', 'd814f6f3', 'ff54d2a6'))
                    # converterGasRecoveryTime_sec = int(self.sql_gasrecovery_time(i, 'a8aef6e9', 'd814f6f3', '4e724ef6'))
                    # smeltv1['converterGasRecoveryTime'] = converterGasRecoveryTime_min + float(
                    #     converterGasRecoveryTime_sec / 60)
                    # print('煤气回收时间', smeltv1['converterGasRecoveryTime'])

                    # 停炉时间
                    smeltv1['temporary_begin'] = STOP_start_time_list[i]
                    print('炉次完成开始时间', smeltv1['temporary_begin'])
                    smeltv1['temporary_end'] = STOP_start_time_list[i]
                    print('炉次完成结束时间', smeltv1['temporary_end'])

                    # 冶炼周期累积
                    smeltPeriod_min = int(self.sql_stop_time_plusone_value(i, 'e150a8de'))
                    smeltPeriod_sec = int(self.sql_stop_time_plusone_value(i, '9ca47ee5'))
                    print('炉次时间分:', smeltPeriod_min)
                    print('炉次时间秒:', smeltPeriod_sec)
                    smeltv1['smeltPeriod'] = smeltPeriod_min + float(smeltPeriod_sec / 60)
                    print('炉次周期时间', smeltv1['smeltPeriod'])

                    # 停炉时间累积
                    temporary_min = int(self.sql_stop_time_value(i, 'e02914b1'))
                    temporary_sec = int(self.sql_stop_time_value(i, 'bcff27ea'))
                    print('停炉时间分:', temporary_min)
                    print('停炉时间秒:', temporary_sec)
                    smeltv1['temporary'] = temporary_min + float(temporary_sec / 60)
                    print('炉次完成时间', smeltv1['temporary'])

                    # 等废钢时间   炉次时间 - 炉次周期时间
                    print('炉次周期，完成时间', smeltv1['cycle'], smeltv1['smeltPeriod'])
                    smeltv1['wateScrap'] = float(smeltv1['cycle']) - float(smeltv1['smeltPeriod'])
                    print("smeltv1['heatNo']的数据类型", type(smeltv1['heatNo']), smeltv1['heatNo'])

                    if smeltv1['heatNo'] != 0:
                        self.wirte_sql()  # 写入82数据库
                        pass

                # 判断副枪使用
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

                inData = self.date_today
                # inData = time.strftime("%Y-%m-%d", time.localtime())
                print(inData)

                sql = "SELECT smeltPeriod_begin,smeltPeriod_end,heatNo FROM `smeltinfo` WHERE inDate = '" + inData + "' "
                print(sql)
                self.cur.execute(sql)
                myresult = self.cur.fetchall()
                for i in myresult:
                    print('冶炼周期开始状态时间段', i)
                    # 开始状态周期内，TSC标志最后时间
                    clickhouse_sql = "SELECT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + \
                                     i[0] + "' and ts < '" + i[
                                         1] + "' AND  eid='70b4408a' and v = 1 ORDER BY ts desc LIMIT 1;"
                    sql_values = self.client.execute(clickhouse_sql)
                    # 有最后时间，证明有使用TSC
                    if sql_values:
                        sql = "UPDATE smeltinfo SET TSC_stat=1 WHERE inDate='" + inData + "' and heatNo = " + str(
                            i[2]) + ";"
                        print(sql)
                        self.cur.execute(sql)
                        self.conn.commit()
                        sql_values = sql_values[0][0]
                        # TSC标志最后时间 - 开始状态结束时间 含氧量PPM  有的话就是  使用TSO副枪
                        clickhouse_sql = "SELECT ts ,v FROM power.collect_steelmaking WHERE ts >= '" + str(
                            sql_values) + "' and ts < '" + \
                                         i[1] + "' AND  eid='9a26fef6' and v > 10 ORDER BY ts asc LIMIT 1;"
                        print(clickhouse_sql)
                        TSO_values = self.client.execute(clickhouse_sql)
                        print('TSC标志最后时间 - 开始状态结束时间 含氧量PPM', TSO_values, len(TSO_values))

                        if len(TSO_values) >= 1:
                            sql = "UPDATE smeltinfo SET TSC_stat=1,TSO_Oxygen_Content = " + str(
                                TSO_values[0][1]) + "  WHERE inDate='" + str(inData) + "' and heatNo = " + str(i[2]) + ";"
                            print(sql)
                            self.cur.execute(sql)
                            self.conn.commit()

                    # 没有最后时间，证明没有使用TSO
                    else:
                        sql_values = None

            # 执行中出现
            if process_state == 500:  # 前一日炉次未结束，生产中的炉次信息
                print('前一日炉次未结束，生产中')
                date_tomorrer = Date_Time_Arithmetic.Add_strday_str(self.date_today, 1)
                # 获取时间 前一日炉次时间与目前时间进行处理炉次信息。
                sql = "SELECT DISTINCT ts ,v FROM power.collect_steelmaking WHERE ts > '" + str(
                    self.date_today) + "'  AND  ts < '" + str(
                    date_tomorrer) + "'  AND (eid='129756f8')  and v= 0 ORDER BY ts  desc LIMIT 1 ;"
                print('前一日生产中的sql', sql)
                sql_values = self.client.execute(sql)
                if sql_values:
                    start_time = Date_Time_Arithmetic.Addsec_dateTime_str(sql_values[0][0], 1)
                    self.cycle_processing(start_time=str(start_time), end_time=str(self.time_now), date_today=self.date_today,
                                     i=i)

                    cycle_time = Date_Time_Arithmetic.Endday_sub_Startday_sec(str(start_time), str(self.time_now))
                    print('全周期时间:', cycle_time)
                    print('全周期时间秒级:', cycle_time)
                    cycle_min = float(cycle_time / 60)
                    print('全周期时间分钟级:', cycle_min)
                    smeltv1['cycle'] = cycle_min

                    if smeltv1['heatNo'] != 0:
                        self.wirte_sql()  # 写入82数据库
                        pass
                # 判断写入数据库

            elif process_state == 65535:
                print('出现异常，需查看提示问题!')
            else:
                print('缺少数值,等待到来')

            # region数据清空
            start_time_list = []  # 存储每炉开始时间数组
            end_time_list = []  # 存储每炉结束时间数组
            STOP_start_time_list = []  # 存储停炉每炉开始时间数组
            STOP_end_time_list = []  # 存储停炉结束时间数组
            for i in smeltv1.keys():
                smeltv1[str(i)] = '0'

            print('前一日一次触发结束')

            # endregion

            # time.sleep(60)

            # except Exception as e:
            # print('异常错误', e)
            # file_handle.write(str(self.time_now) + ':\n异常错误:\n' + str(e) +'\n')


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    # 中台数据库连接
    clickhouse_user = 'oa'
    clickhouse_pwd = 'oa'
    clickhouse_host_sq = '10.6.80.47'
    clickhouse_database = 'power'
    client = Client(host=clickhouse_host_sq, user=clickhouse_user, database=clickhouse_database,
                    password=clickhouse_pwd)
    print('中台数据库连接成功')

    # mysql数据库连接
    pool = PooledDB(pymysql, 1, host='10.6.1.82', user='furnace', passwd='furnace', db='120Tconverter2', port=3306)
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
    date_today = datetime.date.today().strftime("%Y-%m-%d")  # 目前日期 str
    print(date_today,type(date_today))
    # date_today = '2023-04-27'

    once_main(client,cur,conn,date_today=date_today)

    conn.close()


    end_time = datetime.datetime.now()
    duration = end_time - start_time
    print('程序开始时间：', start_time)
    print('程序结束时间：', end_time)
    print('程序运行时间：', duration)

    # tuuu = ['2022-10-29','2022-10-30','2022-10-31']
    # for i in tuuu:
    #     date_today = '2022-10-31'
    #     once_main(client,cur,conn,date_today=i)
    #     conn.close()

