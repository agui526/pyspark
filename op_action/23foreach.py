
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 没有返回值，如果需要返回值，用map
    # 在 executor 直接输出， 而不是拉取到 driver 输出， 效率会高一点
    rdd.foreach(lambda x: print(x * 10))

