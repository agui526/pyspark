
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 参数1： 需要几个数据
    # 参数2： 对排序的数据进行更改（ 不会更改数据本身 ）
    result = rdd.takeOrdered(4)
    print(result)
    # output: [1, 2, 3, 4]

    result = rdd.takeOrdered(4, lambda x: -x)
    print(result)
    # output: [10, 9, 8, 7]

