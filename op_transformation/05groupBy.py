
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 2), ('b', 4), ('b', 3)])

    result = rdd.groupBy(lambda x: x[0])

    print(result.collect())
    # output: [('b', <pyspark.resultiterable.ResultIterable object at 0x00000208C681B190>), ('a', <pyspark.resultiterable.ResultIterable object at 0x00000208C681B150>)]

    print(result.map(lambda x: (x[0], list(x[1]))).collect())
    # output: [('b', [('b', 2), ('b', 4), ('b', 3)]), ('a', [('a', 1), ('a', 2)])]

