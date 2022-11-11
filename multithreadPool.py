#!/sur/bin/env python
# coding:utf-8
'''
@File: multithreadPool.py
@author: qimin.zhang
@Date: 2022/11/11
'''
import os, threading
import traceback

from DBUtils.PooledDB import PooledDB
import pymysql, random, time
from queue import Queue


# from twisted.enterprise import adbapi
# from twisted.internet import reactor

# 创建一个有10个连接的mysql连接池.创建并维持10个线程并发写入1000000条随机数据到test.stu表中

class Test(threading.Thread):
    def __init__(self, n):
        super(Test, self).__init__()
        mysql_conf = {
            "host": "127.0.0.1",
            "user": "zqm",
            "passwd": "Gmcc1234",
            "charset": "utf8",
            "db": "sonic",
            "cursorclass": pymysql.cursors.DictCursor
        }

        # 创建一个连接池,连接池初始最多容纳和创建25个连接,当连接池没有可用连接则阻塞
        # 使用连接池可以进行长连接,无需每次操作mysql时都建立连接,节省了建立连接的时间
        self.n = n
        self.pool = PooledDB(pymysql, maxconnections=25, blocking=True, **mysql_conf)
        self.alpha = list("qwertyuiopasdfghjklzxcvbnm")
        self.sex = ["m", "s"]

    def run(self):
        print("%s号线程开始任务" % self.n)
        sql = "insert into cc_test1 (name,sex,age,classid) values (%s,%s,%s,%s)"

        # 获取连接
        conn = self.pool.connection()
        cursor = conn.cursor()

        data_set = []
        try:
            for i in range(1000):
                name = "".join(random.sample(self.alpha, 5))
                sex = random.choice(self.sex)
                age = random.randint(10, 60)
                classid = random.randint(1000, 9999)
                data_set.append((name, sex, age, classid))

            cursor.executemany(sql, data_set)  # 批量操作,提高效率
            conn.commit()
            print("%s号线程完成任务" % self.n)
        except:
            # 如果出现错误,要回滚
            conn.rollback()
            print("%s号线程任务失败" % self.n)
            traceback.print_exc()
        finally:
            # 无论插入成功还是失败,记得将连接放回连接池供其他线程使用,否则该线程会一直被占用
            cursor.close()
            conn.close()  # 执行完sql操作后,将连接放回连接池,而不是真的关闭连接.如果不放回连接池,则该连接一直处于占用状态,其他线程就无法使用该连接


# 最多创建10个线程并发执行
start_time = time.time()
thread_list = []  # 创建线程池
for i in range(1000):
    thread_list.append(Test(i + 1))
    if len(thread_list) >= 10:  # 当列表中的线程有10个,就开始执行10个线程
        print(len(thread_list))
        print(i)
        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()  # 10个线程都等待执行完,也就是说,10个线程有一个线程没运行完就不能往下执行代码;
            # 这里会阻塞后面的thread_list=[]和print。但是多个线程间的join和join不会阻塞，
            # 也就是说执行完一个join还可以马上执行下一个join，但是执行完最后一个join不能马上执行 thread_list=[]

        thread_list = []  # 当所有线程运行完清空线程池

print("总共用时:" + str(time.time() - start_time))
#总共用时:120.50377607345581（线程10个，数据一次性1百万，耗时很长）
#总共用时:14.55455207824707（线程1000个，数据一次性1一千）

