from pyspark import SparkConf, SparkContext

# application -> job -> stage -> task
conf = SparkConf().setAppName('wordCount').setMaster('local[*]')

sc = SparkContext(conf=conf)
sc.setCheckpointDir(r'data/cache')


# sc = SparkContext()

file_rdd = sc.textFile(r'../data/input/words.txt')

word_rdd = file_rdd.flatMap(lambda line: line.split(' '))

word_rdd.cache()
word_rdd.checkpoint()

word_one_rdd = word_rdd.map(lambda x: (x, 1))

result_rdd = word_one_rdd.reduceByKey(lambda a, b: a + b)

print(result_rdd.collect())
word_rdd.unpersist()

# checkpoint 和 缓存 的区别
# checkpoint 是集中存储的，所以集群模式可以存到hdfs， local模式支持本地文件。 缓存 是分散存储的，存在所在executor的内存或者硬盘上
# checkpoint 不管分区多少个，风险是一样的。 缓存 分区越多，风险越高，因为是分散存储的，一个缓存丢失，其他缓存就没有意义了
# checkpoint 不支持内存存储， 还需要网络IO， 性能上缓存要好一些
# checkpoint 因为涉及认为是安全的，所以不会保留rdd间的血缘关系。 缓存在设计上认为是不安全的，所以会保留rdd间的血缘关系



