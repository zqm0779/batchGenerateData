#!/sur/bin/env python
# coding:utf-8
'''
@File: InsertMany.py
@author: qimin.zhang
@Date: 2022/11/11
'''

# -*- coding:utf-8 -*-
import time
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
    #将待插入的数据先进行遍历，处理放到一个元祖的列表里面
    usersvalues = []
    for num in range(0, 1000000):
        usersvalues.append((num,'zqm','632727349@qq.com'))

    conn = connect(host='127.0.0.1',user='zqm',password='Gmcc1234',database='sonic',charset='utf8')
    cs = conn.cursor()  # 获取光标
    # 注意这里使用的是executemany而不是execute，下边有对executemany的详细说明
    cs.executemany('insert into user2 (uid,uname,email) values(%s,%s,%s)', usersvalues)
    conn.commit()
    cs.close()
    conn.close()
    print('OK')
#这次插入1000000条数据耗时 :  10.306955099105835
if __name__ == '__main__':
    add_test_users()