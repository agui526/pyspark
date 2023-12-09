# coding: utf-8
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as Func

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    rdd = sc.parallelize([[1], [2], [3]])

    df = rdd.toDF(['num'])

    def process(num):
        return {'num': num, 'letters': string.ascii_letters[num]}

    dicType = StructType().add('num', IntegerType(), nullable=False)\
        .add('letters', StringType(), nullable=True)
    # 返回值是 字典的话，需要用 StructType 来定义
    my_udf1 = spark.udf.register('udf1', process, dicType)

    df.selectExpr('udf1(num)').show()



