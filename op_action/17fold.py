
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)

    # 和 reduce 的区别是 fold有初始值， 这个初始值会分别作用在 分区内 和 分区间
    result = rdd.fold(100, lambda a, b: a + b)

    print(result)
    # output: 421
    # 数据分布在3个分区， 每个分区聚合都会加上初始值100， 分区间聚合还会加上一个初始值100， 所以结果是421

