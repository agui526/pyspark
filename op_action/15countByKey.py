
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(r'../data/input/words.txt')

    # 针对 kv型数据，自动分区, 指定了numPartitions>1时，只能保证分区内有序，如果要全局有序，设置为1
    rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))

    result = rdd2.countByKey()

    print(type(rdd2))
    # output: <class 'pyspark.rdd.PipelinedRDD'>
    print(type(result))
    # output: <class 'collections.defaultdict'>
    print(result)
    # output: defaultdict(<class 'int'>, {'hello': 3, 'spark': 3, 'pycharm': 1, 'hadoop': 2, 'yarn': 2, 'flink': 1, 'on': 1})

