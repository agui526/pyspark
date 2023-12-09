from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3])

    rdd2 = sc.parallelize([4, 2, 5])

    print(rdd1.union(rdd2).collect())
    # output: [1, 2, 3, 4, 2, 5]
