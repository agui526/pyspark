
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)
    print(rdd.glom().collect())
    # output: [[1, 2], [3, 4], [5, 6], [7, 8, 9, 10]]

    # 对 rdd 的分区执行重新分区（ 仅数量, 规则不变 ）， 底层调用的就是coalesce
    # 尽量不用， 修改分区会影响并行计算( 内存迭代的并行管道数量 )， 分区如果增加，极大可能导致 shuffle
    result = rdd.repartition(5)
    print(result.glom().collect())
    # output: [[], [], [], [7, 8, 9, 10], [1, 2, 3, 4, 5, 6]]

    result = rdd.repartition(1)
    print(result.glom().collect())
    # output: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]


