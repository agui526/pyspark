
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 随机抽样 rdd 的数据
    # 参数1：True表示允许取同一个数据， False 便是不允许取同一个数据，和数据内容不管，是否重复表示的是同一个位置的数据
    # 参数2：抽样要几个
    # 参数3：随机数种子，传入一个数字即可， 默认会自动给一个随机数
    result = rdd.takeSample(True, 20)
    print(result)
    # output: [7, 10, 10, 10, 1, 10, 1, 3, 9, 10, 4, 9, 6, 1, 2, 1, 4, 1, 1, 2]

    result = rdd.takeSample(False, 20)
    print(result)
    # output: [1, 8, 5, 10, 2, 4, 3, 7, 6, 9]

    result = rdd.takeSample(True, 5)
    print(result)
    # output: [10, 7, 9, 2, 6]

    # 不给定种子，每次抽样都是随机的
    result = rdd.takeSample(False, 5)
    print(result)
    # output: [4, 8, 3, 9, 2]

    result = rdd.takeSample(False, 5)
    print(result)
    # output: [7, 6, 9, 1, 2]

    # 给定种子，每次抽样的结果都是一样的
    result = rdd.takeSample(False, 5, 1)
    print(result)
    # output: [7, 9, 10, 8, 6]

    result = rdd.takeSample(False, 5, 1)
    print(result)
    # output: [7, 9, 10, 8, 6]

