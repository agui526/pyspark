# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    schema = StructType() \
        .add('data', StringType(), nullable=True)

    # json 文件自带 schema 信息，不需指定schema
    df = (spark.read.format('json')
          .load(r'../data/input/stu.json'))

    df.printSchema()
    df.show(3)

    # df1 = spark.read.text(r'../data/input/stu_score.txt', lineSep=',')
    # df1.printSchema()
    # df1.show(3)

