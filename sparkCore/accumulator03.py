from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    count = sc.accumulator(0)

    def process(data):
        global count
        count += 1

    rdd2 = rdd.map(process)
    rdd2.collect()

    rdd3 = rdd2.map(lambda x: x)
    rdd3.collect()

    print(count)
    # output: 20, rdd2.collect后rdd2就被回收了，下面在生成rdd3的时候，会根据血缘关系重新生成rdd2， 这个时候可以用cache

