#!/sur/bin/env python
# coding:utf-8
'''
@File: SingleThreadInsert.py
@author: qimin.zhang
@Date: 2022/11/11
'''

# -*- coding:utf-8 -*-
import time
import traceback

from pymysql import *

# 装饰器，计算插入10000条数据需要的时间
def timer(func):
    def decor(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        d_time = end_time - start_time
        print("这次插入1000000条数据耗时 : ", d_time)
    return decor

@timer
def add_test_users():
    conn = connect(host='127.0.0.1',user='zqm',password='Gmcc1234',database='sonic',charset='utf8')
    cs = conn.cursor()
    for num in range(0, 1000000):
        try:
            sql = "insert into user1(uid,uname,email) values(%s,%s,%s);"
            params = (num, "zqm", "632727349@qq.com")
            cs.execute(sql, params)
        except Exception as e:
            print(e)
            traceback.print_exc()
            return
    conn.commit()
    cs.close()
    conn.close()
    print('OK')

#这次插入10000条数据耗时 :  0.9092390537261963
#这次插入1000000条数据耗时 :  57.825303077697754

if __name__ == '__main__':
    add_test_users()