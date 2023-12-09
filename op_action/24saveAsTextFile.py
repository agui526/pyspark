
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 文件数和分区有关
    # 在 executor 直接执行，所以三个分区会有三个文件
    rdd.saveAsTextFile(r'../data/output/out1')

