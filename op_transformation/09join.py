from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(101, 'zhangsan'), (102, 'lisi'), (103, 'wangwu')])
    rdd2 = sc.parallelize([(101, 'develop'), (103, 'hr')])

    # join 算子只能用于 kv 型数据( 二元元祖 )
    # 关联条件按照二元元祖的key进行
    print(rdd1.join(rdd2).collect())
    # output: [(101, ('zhangsan', 'develop')), (103, ('wangwu', 'hr'))]

    print(rdd1.leftOuterJoin(rdd2).collect())
    # output: [(101, ('zhangsan', 'develop')), (102, ('lisi', None)), (103, ('wangwu', 'hr'))]

    print(rdd1.rightOuterJoin(rdd2).collect())
    output: [(101, ('zhangsan', 'develop')), (103, ('wangwu', 'hr'))]

    print(rdd1.fullOuterJoin(rdd2).collect())
    # output: [(101, ('zhangsan', 'develop')), (102, ('lisi', None)), (103, ('wangwu', 'hr'))]
