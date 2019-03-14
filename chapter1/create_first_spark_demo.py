# -*- coding: utf-8 -*-
'''
Created on 2019年3月14日 下午2:47:52
Zhukun Luo
Jiangxi University of Finance and Economics
'''
#导入sparksql包
from pyspark.sql import SparkSession
from pyspark.sql import types as T
#创建一个spark环境
spark = (
    SparkSession.builder
    .master("local[4]")#表示创建一个4个线程
    .appName("Exploring Joins")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)
sc = spark.sparkContext

#创建一个数据模板
schema = T.StructType([#数据格式
    T.StructField("user_id", T.IntegerType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("sex", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
])

data = [#数据
    (1, "ming Li","male", 13), 
    (2, "fang Zhang","female", 12), 
    (2, "hong Wang","female", 1), 
]

user_df = spark.createDataFrame(#创建一个dataframe
    data=data,
    schema=schema
)

user_df.toPandas()
user_df.show()
'''
|user_id|      name|   sex|age|
+-------+----------+------+---+
|      1|   ming Li|  male| 13|
|      2|fang Zhang|female| 12|
|      2| hong Wang|female|  1|
+-------+----------+------+---+
'''