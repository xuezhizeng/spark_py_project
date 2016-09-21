#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhanghe
@software: PyCharm
@file: run.py
@time: 16-9-21 下午1:28
"""


import os
import sys
import re

from pyspark.sql import SparkSession


def get_agents(spark):
    """
    获取 agents 列表
    """
    # SQL statements can be run by using the sql methods provided by `spark`
    agents = spark.sql("SELECT agent FROM wx_access_log")

    for each in agents.collect():
        print each[0]


def get_top_agents(spark, order_by='desc', limit=None):
    """
    获取热门/冷门 agents 列表
    """
    # SQL statements can be run by using the sql methods provided by `spark`
    sql = "SELECT agent, COUNT(1) AS agent_count FROM wx_access_log GROUP BY agent ORDER BY agent_count %s" % order_by
    if limit:
        sql += " limit %s" % limit
    agents_group = spark.sql(sql)

    print '-'*12, '%s Top %s' % ('热门' if order_by == 'desc' else '冷门', limit if limit else ''), '-'*12
    index = 0
    for each in agents_group.collect():
        index += 1
        print '[%2d]' % index, each[1], each[0]


def get_wx_ver(agent):
    """
    获取微信版本号
    """
    wx_rule = r'MicroMessenger/(\d+\.\d+\.\d+)'
    wx_var = re.compile(wx_rule, re.I).findall(agent)
    return wx_var[0] if wx_var else None


def get_wx_ver_list(spark):
    """
    获取 微信主版本 列表
    MicroMessenger/6.3.25.861
    """
    # 注册自定义函数
    spark.udf.register("get_wx_ver", get_wx_ver)
    sql = "SELECT get_wx_ver(agent) as ver, count(*) as c FROM wx_access_log group by 1 ORDER BY c desc"
    wx_vars = spark.sql(sql)
    print '-' * 12, '微信版本排行', '-' * 12
    index = 0
    for wx_var in wx_vars.collect():
        index += 1
        print '[%2d]' % index, wx_var[1], wx_var[0]


def run():
    spark = SparkSession \
        .builder \
        .appName("WxLog") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files.
    if len(sys.argv) < 2:
        path = os.path.join(os.path.dirname(__file__), "https.wx.access.log-20160917")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    wx_log = spark.read.json(path)

    # The inferred schema can be visualized using the printSchema() method.
    wx_log.printSchema()
    # root
    # | -- @ timestamp: string(nullable=true)
    # | -- agent: string(nullable=true)
    # | -- clientip: string(nullable=true)
    # | -- cookie: string(nullable=true)
    # | -- host: string(nullable=true)
    # | -- http_host: string(nullable=true)
    # | -- method: string(nullable=true)
    # | -- referer: string(nullable=true)
    # | -- responsetime: double(nullable=true)
    # | -- size: long(nullable=true)
    # | -- status: string(nullable=true)
    # | -- upstreamhost: string(nullable=true)
    # | -- upstreamtime: string(nullable=true)
    # | -- url: string(nullable=true)

    # Creates a temporary view using the DataFrame.
    wx_log.createOrReplaceTempView("wx_access_log")

    # 获取 agents 列表
    # get_agents(spark)

    # 获取热门/冷门 agents 列表
    # get_top_agents(spark, 'desc', 10)   # 热门
    # get_top_agents(spark, 'asc', 10)    # 冷门

    # 获取微信版本号
    get_wx_ver_list(spark)

    spark.stop()


if __name__ == "__main__":
    # tar zxvf https.wx.access.log-20160917.gz
    run()
