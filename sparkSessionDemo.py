from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = (SparkSession.builder
            .appName('Test')
            .config('spark.sql.shuffle.partitions', '4')).getOrCreate()

    # SparkSession 也可以获取 SparkContext, 作为 sparkCore 的入口
    # sc = spark.sparkContext

    df = spark.read.csv(r'data/input/stu_score.txt', sep=',', header=False)
    df2 = df.toDF('id', 'subject', 'score')
    df2.printSchema()
    df2.show()

    df2.createTempView('stu_score')

    # SQL 风格
    spark.sql('''
        select * from stu_score where id = 102
    ''').show()

    # DSL 风格
    df2.where('subject = "语文"').show()



