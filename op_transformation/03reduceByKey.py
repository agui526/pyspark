
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 2), ('b', 4), ('b', 3)] )

    result = rdd.reduceByKey(lambda a, b: a + b)

    print(result.glom().collect())

