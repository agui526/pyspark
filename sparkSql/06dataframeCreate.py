# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    df = (spark.read.format('csv')
          .option('sep', ',')
          .option('header', True)
          .option('encoding', 'utf-8')
          .schema('stu_id int, subject string, score int')
          .load(r'../data/input/stu_score.csv'))

    df.printSchema()
    df.show(3)
