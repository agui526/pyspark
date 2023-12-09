# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as Func

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    sc = spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7]).map(lambda x: [x])

    df = rdd.toDF(['id'])

    # df.show()

    # 方式21. 创建UDF， spark.udf.register
    def num_times_10(num):
        return num * 10

    # 参数1. UDF 的名称，这个UDF名称仅可以用于SQL风格
    # 参数2. UDF的逻辑实现
    # 参数3. UDF的返回类型，必须声明返回类型
    # register返回值是一个 UDF 对象，该对象只能用于 DSL 风格， 但是 UDF 名称可以用于 SQL 风格
    my_udf = spark.udf.register('udf1', num_times_10, IntegerType())

    df.createOrReplaceTempView('tmp')

    spark.sql('select udf1(id) as res from tmp').show()
    df.selectExpr('udf1(id)').show()
    df.select(my_udf('id')).show()


    # 方式2 创建 UDF,pyspark.sql.functions.udf
    # 仅能用于 DSL 风格
    my_udf2 = Func.udf(num_times_10, IntegerType())
    df.select(my_udf2(df['id'])).show()

