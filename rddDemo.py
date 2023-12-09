# rdd
# 1. 不可变 immutable
#     不可变集合, 变量的申明使用 val
# 2. 分区的 partitioned
#     集合的数据被划分为很多部分，每部分称为分区
# 3. 并行计算 parallel
#     集合的数据可被并行计算处理，每个分区数据被一个Task人物处理

# rdd 是有分区的
# 计算方法都会作用到每一个分区(分片)之上
# rdd之间是有相互依赖关系的
# KV型的rdd可以有分区器
# rdd分区数据的读取会尽量靠近数据所在地, 移动计算而不是移动数据

from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local[*]").setAppName("Test")
sc = SparkContext(conf=conf)

# parallelize() 将本地集合转换成分布式对象， 默认分区数 getNumPartitions(), 等于 CPU 核心数
rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
print(rdd1.getNumPartitions())

# collect() 将rdd中每个分区的数据都拉取到driver中，生成一个python list本地对象
print(rdd1.glom().collect())
# glom() 函数用于将一个分布式数据集（例如RDD）转换为一个新的分布式数据集，其中每个元素都是原始数据集中的一组连续元素。
# 换句话说，它可以将一个扁平的数据结构转换为一个嵌套的数据结构。 和 flatMap() 相反

# KV型rdd可以有分区器，默认分区器： hash 分区规则
# 可以手动设置一个分区器( rdd.partitionBy 方法来设置 )
print('='*50)

file_rdd1 = sc.textFile(r'../data/input/words.txt')
print('rdd1 默认分区:', file_rdd1.getNumPartitions())
print('rdd1 内容: ', file_rdd1.glom().collect())

file_rdd2 = sc.textFile(r'../data/input/words.txt', 3)
print('rdd2 默认分区:', file_rdd2.getNumPartitions())
print('rdd2 内容: ', file_rdd2.glom().collect())

file_rdd3 = sc.textFile(r'../data/input/words.txt', 100)
print('rdd3 默认分区:', file_rdd3.getNumPartitions())
print('rdd3 内容: ', file_rdd3.collect())

print('=' * 50)
# 适合读取一堆小文件
file_rdd4 = sc.wholeTextFiles(r'../data/input/')
print(file_rdd4.getNumPartitions())
print(file_rdd4.glom().collect())

# rdd 算子
# transformation 算子
#     返回值仍然是一个rdd， 这类算子是懒加载的，如果没有action算子，transformation算子是不工作的
# action 算子
#     返回值不是rdd
