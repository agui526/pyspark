from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('b', 2), ('a', 1)], 2)

    # glom 将rdd的数据加上嵌套，这个嵌套是按照分区来的
    print(rdd1.glom().collect())
    # output: [[('a', 1)], [('b', 2), ('a', 1)]]

