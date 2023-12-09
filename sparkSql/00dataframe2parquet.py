# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName('Test'). \
        master('local[*]'). \
        getOrCreate()

    # gzip   lzo  lz4
    spark.conf.set("spark.sql.parquet.compression.codec", "lz4")

    data = [
            ("James ", "", "Smith", "36636", "M", 3000),
            ("Michael ", "Rose", "", "40288", "M", 4000),
            ("Robert ", "", "Williams", "42114", "M", 4000),
            ("Maria ", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
    ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    df = spark.createDataFrame(data, columns)

    # df.show(3)

    df.write.parquet(r"../data/output/person.parquet")
    # df.write.format('parquet').save(r"../data/output/person.parquet")
