from pyspark import SparkConf, SparkContext

# application -> job -> stage -> task
conf = SparkConf().setAppName('wordCount').setMaster('local[*]')

sc = SparkContext(conf=conf)

# sc = SparkContext()

file_rdd = sc.textFile(r'../data/input/words.txt')

word_rdd = file_rdd.flatMap(lambda line: line.split(' '))

word_one_rdd = word_rdd.map(lambda x: (x, 1))

result_rdd = word_one_rdd.reduceByKey(lambda a, b: a + b)

print(result_rdd.collect())

