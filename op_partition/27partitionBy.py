
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop', 1), ('hello', 2), ('flink', 2), ('hadoop', 4), ('spark', 3), ('java', 2)], 4)
    print(rdd.glom().collect())
    # output: [[('hadoop', 1)], [('hello', 2), ('flink', 2)], [('hadoop', 4)], [('spark', 3), ('java', 2)]]

    def process(k):
        if str(k).startswith('h'):
            return 0
        elif str(k).startswith('j'):
            return 1
        return 2

    # 参数1： 重新分区后有几个分区
    # 参数2： 自定义分区规则，传入函数， 返回值必须是 int
    result = rdd.partitionBy(3, process)
    print(result.glom().collect())
    # output: [[('hadoop', 1), ('hello', 2), ('hadoop', 4)], [('java', 2)], [('flink', 2), ('spark', 3)]]


