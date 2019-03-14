# -*- coding: utf-8 -*-
'''
Created on 2019年3月14日 下午2:56:53
Zhukun Luo
Jiangxi University of Finance and Economics
'''
#读取数据集
from pyspark.sql import SparkSession
from pyspark.sql import types as T
'''
SparkSession
spark应用程序的入口。使用配置文件进行配置
pyspark.sql.functions
将多个函数组装成链
需要进行基础功能的转换函数:
    api文档
    Google查询
    用户自定义函数(udf).

pyspark.sql.types
定义每一列数据的数据类型.
'''
#配置上下文环境
spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("Section 2 - read first dataset")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)
sc = spark.sparkContext

# 定义csv数据的内部结构
def get_csv_schema(*args):
    return T.StructType([
        T.StructField(*arg)
        for arg in args
    ])

# 给csv文件添加读取格式
def read_csv(fname, schema):
    return spark.read.csv(
        path=fname,
        header=True,
        schema=get_csv_schema(*schema)
    )

import os
path ='../data/user.csv'
df = read_csv(
    fname=path,
    schema=[
        ("id", T.LongType(), False),
        ("user_id", T.LongType(), True),
        ("name", T.StringType(), True),
        ("birthday", T.TimestampType(), True),
        ("color", T.StringType(), True)
    ]
)
df.toPandas()
df.show()
'''
+---+-------+---------+-------------------+-----+
| id|user_id|     name|           birthday|color|
+---+-------+---------+-------------------+-----+
|  1|      1|  King Li|2014-11-22 12:30:31|    5|
|  2|      3|hong Wang|2016-11-22 10:05:10|   10|
|  3|      1|Che Zhang|2016-11-22 10:05:10|   15|
|  3|      2|Maple Liu|2018-11-22 10:05:10|   17|
|  4|      2|     null|2019-01-01 10:05:10|   13|
+---+-------+---------+-------------------+-----+
'''