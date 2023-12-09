
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    def process(iter):
        res = list()
        for it in iter:
            res.append(it * 10)
        print(res)

    # action 算子
    rdd.foreachPartition(process)
    # output: [70, 80, 90, 100]
    #     [10, 20, 30]
    #     [40, 50, 60]

