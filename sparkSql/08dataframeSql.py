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

    id_column = df['sid']
    print(id_column)
    subject_column = df['subject']
    score_column = df['score']

    df.select('sid', 'subject', 'score').show(3)
    df.select(['sid', 'subject']).show(3)
    df.select(id_column, subject_column).show(4)
    df.select([id_column, score_column]).show(4)

    df.where('score > 95').show()
    df.where(df['score'] > 95).show()

    df.filter('score > 95').show()
    df.filter(df['score'] > 95).show()

    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()
