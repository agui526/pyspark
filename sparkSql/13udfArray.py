# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as Func

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    rdd = sc.parallelize([['hadoop spark pyspark'], ['hello sql flink'], ['python scala java']])

    df = rdd.toDF(['line'])

    def splitLine(line):
        return line.split(' ')

    my_udf1 = spark.udf.register('udf1', splitLine, ArrayType(StringType()))

    df.selectExpr('udf1(line)').show(truncate=False)

    df.select(my_udf1(df['line'])).show()

    my_udf2 = Func.udf(splitLine, ArrayType(StringType()))

    df.select(my_udf2('line')).show(truncate=False)

