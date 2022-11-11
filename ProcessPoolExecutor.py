#!/sur/bin/env python
# coding:utf-8
'''
@File: ProcessPoolExecutor.py
@author: qimin.zhang
@Date: 2022/11/11
'''

import pymysql
import requests,time
from concurrent.futures import ProcessPoolExecutor

def data_handler(urls):
    conn = pymysql.connect(host='127.0.0.1',user='zqm',password='Gmcc1234',database='sonic',charset='utf8')
    cursor = conn.cursor()
    for i in range(urls[0], urls[1]):
        sql = 'insert into user3(uid,uname,email) values(%s,concat(%s,"zqm"),concat(%s,"zqm","632727349@qq.com"));'
        res = cursor.execute(sql, [i, i, i])
        conn.commit()
    cursor.close()
    conn.close()

def run():
    urls = [(1,2000000),(2000001,5000000),(5000001,8000000),(8000001,10000000)]
    with ProcessPoolExecutor() as excute:
        ##ProcessPoolExecutor 提供的map函数，可以直接接受可迭代的参数，并且结果可以直接for循环取出
        excute.map(data_handler,urls)

if __name__ == '__main__':
    start_time = time.time()
    run()
    stop_time = time.time()
    print('插入千万条数据耗时 %s' % (stop_time - start_time))

    #插入1百万条数据耗时 78.20944690704346
    #插入千万条数据耗时：771.2906413078308
