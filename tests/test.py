# _*_coding:utf-8_*_
import pymysql
import time, datetime, random
import threading

def trade():
    conn = pymysql.connect(host='localhost', port=3306, user='lianghuaxiong', password='lianghuaxiong', charset='utf8')
    cursor = conn.cursor()
    for i in range(5):
        cursor.execute(f'call bitcoin.p_trade();')
        print('111111111    '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        time.sleep(1)
    conn.commit()
    cursor.close()
    conn.close()


def insert_order():
    conn = pymysql.connect(host='localhost', port=3306, user='lianghuaxiong', password='lianghuaxiong', charset='utf8')
    cursor = conn.cursor()
    for i in range(10):
        cursor.execute(f'insert into bitcoin.order_sell values ({random.randint(1, 10)},{random.randint(1, 5)},{random.randint(10, 30)},now(6));')
        time.sleep(0.1)
        cursor.execute(f'insert into bitcoin.order_buy  values ({random.randint(11, 20)},{random.randint(5, 10)},{random.randint(10, 30)},now(6));')
        print('22222   '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
    conn.commit()
    cursor.close()
    conn.close()



# conn = pymysql.connect(host='localhost', port=3306, user='lianghuaxiong', password='lianghuaxiong', charset='utf8')
# cursor = conn.cursor()
# # t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')



thread1 = threading.Thread(target=insert_order)
thread2 = threading.Thread(target=trade)

# 线程开始
thread1.start()
thread2.start()

# 线程结束
thread1.join()
thread2.join()



print('==============')
# conn.commit()
# cursor.close()
# conn.close()









