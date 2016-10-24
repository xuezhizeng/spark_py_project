#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhanghe
@software: PyCharm
@file: __init__.py.py
@time: 2016/10/24 下午6:40
"""


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
