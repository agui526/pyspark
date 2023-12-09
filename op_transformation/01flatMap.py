from pyspark import SparkContext


sc = SparkContext("local", "flatMap example")

# 创建一个包含元组的RDD，每个元组包含一个字符串和一个整数列表
data = [(("a", [1, 2, 3]),), (("b", [4, 5]), ("c", [6]))]
rdd = sc.parallelize(data)

# 使用flatMap操作符将整数列表连接成一个单一的整数列表，并将所有整数列表连接成一个更大的整数列表
result = (rdd
          .flatMap(lambda x: x)
          .flatMap(lambda x: x[1])
          .map(lambda x: x * 2)
          .reduce(lambda a, b: a + b))

print(result)
