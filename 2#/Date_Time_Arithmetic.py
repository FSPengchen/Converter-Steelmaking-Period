import datetime
import time

'''
更新于 2022.05.09
'''


#MYSQL 转换方式
#20210604004621.830 转换为 2021-06-04 00:46:21
def Str_Convert_DateTime(inputstrdate):
    strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
    startTime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")  # 把strTime转化为时间格式,后面的秒位自动补位的
    return startTime

#MYSQL 转换方式
#20210604004621.830 转换为 2021-06-04 00:46:21  加分钟 转 20210604004621.830
def DateTime_Addmin(inputstrdate,n):
    strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
    addtime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")  #吧strtime格式转换为 datetime 格式
    return (addtime + datetime.timedelta(minutes=n)).strftime("%Y%m%d%H%M%S")+"." +inputstrdate[15:19]


#MYSQL 转换方式
#20210604004621.830 转换为 2021-06-04 00:46:21  加天 转 20210604004621.830
def DateTime_Addday(inputstrdate,n):
    strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
    addtime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")
    return (addtime + datetime.timedelta(days=n)).strftime("%Y%m%d%H%M%S")+"." +inputstrdate[15:19]

#20210604004621.830 转换为 2021-06-04 00:46:21  加秒 转 20210604004621.830
def DateTime_Addsec(inputstrdate,n):
    strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
    addtime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")
    return (addtime + datetime.timedelta(seconds=n)).strftime("%Y%m%d%H%M%S") +"." +inputstrdate[15:19]

# def con_Time(inputstrdate):
#     strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
#     addtime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")
#     return addtime
#
#
#2021-06-04 00:46:21 str 转换为 datetime  加天  转 str
def Addday_dateTime_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday + datetime.timedelta(days=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d %H:%M:%S")
#
#2021-06-04 00:46:21 str 转换为 datetime  加秒  转 str
def Addsec_dateTime_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday + datetime.timedelta(seconds=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d %H:%M:%S")
#
#2021-06-04 00:46:21 str 转换为 datetime  减秒  转 str
def Subsec_dateTime_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday - datetime.timedelta(seconds=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d %H:%M:%S")

#2021-06-04 00:46:21 str 转换为 datetime  加分钟  转 str
def Addmin_dateTime_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday + datetime.timedelta(minutes=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d %H:%M:%S")

def Submin_dateTime_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday - datetime.timedelta(minutes=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d %H:%M:%S")


#2021-06-04  str 转 datetime 加天 转str
def Add_strday_str(inputstrdate,n):
    # print(inputstrdate,type(inputstrdate))
    datetimeday = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d")
    # print(datetimeday,type(datetimeday))
    datetimestrday = datetimeday + datetime.timedelta(days=n)
    # print(datetimestrday,type(datetimestrday))
    return datetimestrday.strftime("%Y-%m-%d")

# 2021-06-04  str 转 datetime 相减
def Endday_sub_Startday_min(Startday,Endday):
    # str 格式'2021-06-05 00:45:45'转换为 datetime
    Startdaytime = datetime.datetime.strptime(Startday,"%Y-%m-%d %H:%M:%S")
    Enddaytime = datetime.datetime.strptime(Endday, "%Y-%m-%d %H:%M:%S")
    # datetime - datetime 相减
    a = Enddaytime - Startdaytime
    # print(a)
    # print('a.days',a.days)
    # print('a.seconds',a.seconds)
    b = int(a.days)*24*60*60 + int(a.seconds)
    # print(b, b/60)
    c = int(b /60)
    #只支持day 和seconds
    return c

def Endday_sub_Startday_sec(Startday,Endday):
    # str 格式'2021-06-05 00:45:45'转换为 datetime
    Startdaytime = datetime.datetime.strptime(Startday,"%Y-%m-%d %H:%M:%S")
    Enddaytime = datetime.datetime.strptime(Endday, "%Y-%m-%d %H:%M:%S")
    # datetime - datetime 相减
    a = Enddaytime - Startdaytime
    return_sec= (int(a.days) * 86400 )+ (int(a.microseconds) * 60) + int(a.seconds)
    #只支持day 和seconds
    return return_sec

# print(timeCon.str_con_date("2021-06-22 00:13:06",1),type(timeCon.str_con_date("2021-06-22 00:13:06",0)))
def str_con_date(inputstrdate,n):
    # print("年",inputstrdate[:4]) #年
    # print("月",inputstrdate[5:7])#月
    # print("日",inputstrdate[8:10])#日
    # print("小时",inputstrdate[11:13])#小时
    # print("分钟",inputstrdate[14:16])#分钟
    # print("秒",inputstrdate[17:19])#秒
    # print("秒", inputstrdate[:10])
    startTime = datetime.datetime.strptime(inputstrdate, "%Y-%m-%d %H:%M:%S")
    datetimestrday = startTime + datetime.timedelta(days=n)
    # startTime = datetime.datetime.strptime(startTime, "%Y-%m-%d")

    return str(datetimestrday)[:10]


# result = timeCon.compare_time('2017-04-17', '2017-04-17')
# print('the compare result is:', result, type(result))
def compare_time(time1, time2):
    s_time = time.mktime(time.strptime(time1, '%Y-%m-%d'))
    e_time = time.mktime(time.strptime(time2, '%Y-%m-%d'))
    # print('s_time is:', s_time)
    # print('e_time is:', e_time)
    return int(s_time) - int(e_time)


#print(timeCon.Time_Show("20210628060947.930"))
def Time_Show(inputstrdate):
    print("年",inputstrdate[:4]) #年
    print("月",inputstrdate[4:6])#月
    print("日",inputstrdate[6:8])#日
    print("小时",inputstrdate[8:10])#小时
    print("分钟",inputstrdate[10:12])#分钟
    print("秒",inputstrdate[12:14])#秒

    strTime = inputstrdate[:4]+'-'+inputstrdate[4:6]+'-'+inputstrdate[6:8]+' '+inputstrdate[8:10]+':'+inputstrdate[10:12]+':'+inputstrdate[12:14]
    print("字符串转换为指定格式",strTime)
    # strTime = '2019-07-11 11:03'  # 给定一个时间，此是个字符串
    startTime = datetime.datetime.strptime(strTime, "%Y-%m-%d %H:%M:%S")  # 把strTime转化为时间格式,后面的秒位自动补位的

    print("字符串转换datetime格式",startTime,type(startTime))

    print("datetime转换字符串",startTime.strftime("%Y-%m-%d %H:%M"),type(startTime.strftime("%Y-%m-%d %H:%M:%S")))  # 格式化输出，保持和给定格式一致
    # startTime时间加 一分钟
    #startTime2 = (startTime + datetime.timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
    startTime2 = (startTime + datetime.timedelta(minutes=20)).strftime("%Y%m%d%H%M%S")
    print(startTime2 +"." +inputstrdate[15:19])

#2021-06-28 转换 20210628
# print(timeCon.date_con_day("2021-06-28"))
def date_con_day(inputstrdate):
    # print("年",inputstrdate[:4]) #年
    # print("月",inputstrdate[5:7])#月
    # print("日",inputstrdate[8:10])#日
    strTime = inputstrdate[:4]+inputstrdate[5:7]+inputstrdate[8:10]
    # print("字符串转换为指定格式",strTime)
    return strTime

#2021-06-28 01:51:09  转换为 20210628122756.863
#    print(timeCon.date_con_mySql("2021-06-28 01:52:19"))
def date_con_mySql(inputstrdate):
    # print("年",inputstrdate[:4]) #年
    # print("月",inputstrdate[5:7])#月
    # print("日",inputstrdate[8:10])#日
    # print("小时",inputstrdate[11:13])#小时
    # print("分钟",inputstrdate[14:16])#分钟
    # print("秒",inputstrdate[17:19])#秒
    strTime = inputstrdate[:4] + inputstrdate[5:7] + inputstrdate[8:10] + inputstrdate[11:13] + inputstrdate[14:16] + inputstrdate[17:19] +".000"
    return strTime


if __name__ == '__main__':

    # 20210604004621.830 转换为 2021-06-04 00:46:21
    # print(Str_Convert_DateTime("20210604004621.830"))

    # 20210604004621.830 转换为 2021-06-04 00:46:21  加分钟 转 20210604004621.830
    # print(DateTime_Addmin("20210604004621.830", 2))

    # 20210604004621.830 转换为 2021-06-04 00:46:21  加天 转 20210604004621.830
    # print(DateTime_Addday('20210604004621.830', 2))

    # 20210604004621.830 转换为 2021-06-04 00:46:21  加秒 转 20210604004621.830
    # print(DateTime_Addsec('20210604004621.830', 2))

    # 2021-06-04 00:46:21 str 转换为 datetime  加天  转 str
    # print(Addday_dateTime_str('2021-06-04 00:46:21', 2))

    # print(Endday_sub_Startday_sec('2021-06-04 00:46:21','2021-07-04 00:46:21'))

    print(Endday_sub_Startday_min('2021-12-21 1:46:21', '2021-12-22 3:56:21'))


    # #datetime  转 str
    # nowday = datetime.date.today().strftime("%Y-%m-%d")
    # print(nowday, type(nowday))

    # #str 转 datetime
    # ct = datetime.datetime.strptime(nowday,'%Y-%m-%d')
    # print(ct)
    # print(type(ct))
    # print(timeCon.date_con_mySql("2021-06-28 01:52:19"))
    # print("20210628122756.863")
    # print(timeCon.date_con_day("2021-06-28"))
    # print(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))










