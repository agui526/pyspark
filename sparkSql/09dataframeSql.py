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
          .schema('sid int, subject string, score int')
          .load(r'../data/input/stu_score.csv'))

    df.createTempView('stu_score')
    df.createOrReplaceTempView('stu_score')
    df.createGlobalTempView('g_stu_score')
    # 全局临时视图在使用的时候，要加上 global_temp, 可以在多个 sparkSession 中使用

    spark.sql('select * from stu_score where score >= 95').show()
    spark.sql('select * from global_temp.g_stu_score where score >= 95').show()
    spark.sql('select sid, max(score) from global_temp.g_stu_score group by sid').show()

