
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 2), ('b', 4), ('b', 3), ('c', 5)])

    # 指定了numPartitions>1时，只能保证分区内有序，如果要全局有序，设置为1
    result = rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=2)

    print(result.collect())
    # output: [('a', 1), ('b', 2), ('a', 2), ('b', 3), ('b', 4), ('c', 5)]

