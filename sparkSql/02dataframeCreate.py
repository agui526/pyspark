# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    # 基于 rdd 转换成 DataFrame
    rdd = sc.textFile(r'../data/input/stu_score.txt'). \
        map(lambda x: x.split(',')). \
        map(lambda x: (int(x[0].strip()), x[1].strip(), int(x[2].strip())))

    schema = StructType() \
        .add('stu_id', IntegerType(), nullable=True) \
        .add('subject', StringType(), nullable=True) \
        .add('score', IntegerType(), nullable=True)

    df = spark.createDataFrame(rdd, schema)

    df.printSchema()
    df.show(3)
