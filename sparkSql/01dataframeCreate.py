# coding: utf-8

from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.\
        appName('Test').\
        master('local[*]').\
        getOrCreate()

    sc = spark.sparkContext

    # 基于 rdd 转换成 DataFrame
    rdd = sc.textFile(r'../data/input/stu_score.txt').\
        map(lambda x: x.split(',')).\
        map(lambda x: (x[0], x[1].strip(), int(x[2])))

    df = spark.createDataFrame(rdd, schema=['stu_id', 'subject', 'score'])

    df.printSchema()

    df.show(10, False)
