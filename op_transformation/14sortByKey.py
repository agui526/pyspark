
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 2), ('b', 4), ('b', 3), ('c', 5)])

    # 针对 kv型数据，自动分区, 指定了numPartitions>1时，只能保证分区内有序，如果要全局有序，设置为1
    result = rdd.sortByKey(ascending=False, numPartitions=1)

    print(result.collect())
    # output: [('c', 5), ('b', 2), ('b', 4), ('b', 3), ('a', 1), ('a', 2)]

    result = rdd.sortByKey(ascending=False, numPartitions=2)

    print(result.glom().collect())
    # output: [[('c', 5)], [('b', 2), ('b', 4), ('b', 3), ('a', 1), ('a', 2)]]

    # 第三个参数 keyfunc， 传进去的只有key, 好像不起作用？？？
    result = rdd.sortByKey(ascending=False, numPartitions=1, keyfunc=lambda key: key.upper())

    print(result.collect())
    # output: [('c', 5), ('b', 2), ('b', 4), ('b', 3), ('a', 1), ('a', 2)]
