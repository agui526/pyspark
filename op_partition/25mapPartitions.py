
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    def process(iter):
        res = list()
        for it in iter:
            res.append(it * 10)
        return res

    # map 和 mapPartitions 的区别， map 每次传输一条数据， mapPartitions 每次传输整个分区的数据， 作为一个list传入
    # transformation 算子
    result = rdd.mapPartitions(process).collect()

    print(result)

