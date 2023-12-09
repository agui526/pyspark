from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    count = sc.accumulator(0)

    def process(data):
        global count
        count += 1
        print(count)


    print(rdd.map(process).collect())
    print(count)
    # output: 10

