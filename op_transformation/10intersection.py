from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('b', 2), ('a', 1)])

    rdd2 = sc.parallelize([('a', 2), ('b', 2), ('a', 1)])

    print(rdd1.intersection(rdd2).collect())
    # output: [('a', 1), ('b', 2)]  会去重
