
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)
    print(rdd.glom().collect())
    # output: [[1, 2], [3, 4], [5, 6], [7, 8, 9, 10]]

    # 增加分区。 必须显示指定第二个参数为 True， 即允许 shuffle
    result = rdd.coalesce(5)
    print(result.glom().collect())
    # output: [[1, 2], [3, 4], [5, 6], [7, 8, 9, 10]], 没有指定第二个参数，没有生效

    result = rdd.coalesce(5, True)
    print(result.glom().collect())
    # output: [[], [], [], [7, 8, 9, 10], [1, 2, 3, 4, 5, 6]]

    result = rdd.coalesce(1)
    print(result.glom().collect())
    # output:


