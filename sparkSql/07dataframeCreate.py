# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    # parquet 是spark中常用的一种列式存储文件格式， 和 hive中的 orc差不多
    # parquet和普通文本的区别
    # 1. parquet内置schema（列明，列类型，是否为空）
    # 2. 存储是以列作为存储格式
    # 3. 存储是序列化存储在文件中的（有压缩属性，体积小）
    df = (spark.read.format('parquet')
          .load(r'../data/input/stu_score.csv'))

    df.printSchema()
    df.show(3)
