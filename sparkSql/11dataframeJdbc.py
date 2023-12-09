# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as func

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

    df.write.mode('overwrite').format('jdbc').\
        option('url', 'jdbc:mysql://localhost/ssmdemo?useSSL=false&useUnicode=true&allowPublicKeyRetrieval=true').\
        option('dbtable', 'stu_score').\
        option('user', 'root').\
        option('password', 'root').\
        save()


