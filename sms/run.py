#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhanghe
@software: PyCharm
@file: run.py.py
@time: 16-9-27 下午4:28
"""

import os
import sys
import datetime

from pyspark.sql import SparkSession

log_path = os.path.dirname(__file__)


def get_valid_send(spark, start_time, end_time):
    """
    验证码发送记录
    """
    sql = "SELECT source, count(DISTINCT mobile), COUNT(*) FROM valid_send_log where `timestamp`>=\'%s\' and `timestamp`<=\'%s\' GROUP BY source" % (start_time, end_time)
    print sql
    wx_vars = spark.sql(sql)
    print '-' * 12, '验证码发送记录', '-' * 12
    print '\t'.join(['source', 'mobile_uv', 'send_num'])
    for row in wx_vars.collect():
        print '\t'.join([str(item) for item in list(row)])


def get_valid_result(spark, start_time, end_time):
    """
    验证码结果记录
    """
    sql = "SELECT source, result, count(DISTINCT mobile), COUNT(*) FROM valid_result_log where `timestamp`>=\'%s\' and `timestamp`<=\'%s\' GROUP BY source, result" % (start_time, end_time)
    print sql
    wx_vars = spark.sql(sql)
    print '-' * 12, '验证码结果记录', '-' * 12
    print '\t'.join(['source', 'result', 'mobile_uv', 'send_num'])
    for row in wx_vars.collect():
        print '\t'.join([str(item) for item in list(row)])


def run():
    """
    spark-submit --master=spark://ThinkPad-L421:7077 src/welog/analyse/sms.py
    spark-submit --master=spark://ThinkPad-L421:7077 src/welog/analyse/sms.py 2016-09-23
    spark-submit --master=spark://ThinkPad-L421:7077 src/welog/analyse/sms.py 2016-09-22 2016-09-23
    """
    # print sys.argv
    spark = SparkSession \
        .builder \
        .appName("SmsLog") \
        .getOrCreate()

    valid_send_path = os.path.join(log_path, "valid.send.*.json")
    valid_send = spark.read.json(valid_send_path)
    # valid_send.printSchema()
    valid_send.createOrReplaceTempView("valid_send_log")

    valid_result_path = os.path.join(log_path, "valid.result.*.json")
    valid_result = spark.read.json(valid_result_path)
    # valid_result.printSchema()
    valid_result.createOrReplaceTempView("valid_result_log")

    if len(sys.argv) == 2:
        start_time = sys.argv[1] + 'T00:00:00+08:00'
        end_time = sys.argv[1] + 'T23:59:59+08:00'
    elif len(sys.argv) == 3:
        start_time = sys.argv[1] + 'T00:00:00+08:00'
        end_time = sys.argv[2] + 'T23:59:59+08:00'
    else:
        # 默认统计昨天一天的数据
        d = datetime.datetime.now() + datetime.timedelta(days=-1)
        d_str = d.strftime('%Y-%m-%d')
        start_time = d_str + 'T00:00:00+08:00'
        end_time = d_str + 'T23:59:59+08:00'

    # 业务逻辑
    get_valid_send(spark, start_time, end_time)
    get_valid_result(spark, start_time, end_time)

    spark.stop()


if __name__ == '__main__':
    run()
