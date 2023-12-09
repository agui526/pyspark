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

    # text 格式默认是整行数据当做一列来处理，不设置schema，默认列名是 value， 类型是 string
    df = (spark.read.format('text')
          # .schema(schema=schema)
          .load(r'../data/input/stu_score.txt'))

    df.printSchema()
    df.show(3)

    # df1 = spark.read.text(r'../data/input/stu_score.txt', lineSep=',')
    # df1.printSchema()
    # df1.show(3)

