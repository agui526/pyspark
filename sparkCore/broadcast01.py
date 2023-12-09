from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Test")
    sc = SparkContext(conf=conf)

    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓鸥', 13),
                     (3, '王大力', 11),
                     (4, '赵本山', 11)]

    score_info_rdd = sc.parallelize([
        (1, '语文', 90), (1, '数学', 98), (1, '英语', 80),
        (2, '语文', 94), (2, '数学', 90), (2, '英语', 89),
        (3, '语文', 87), (3, '数学', 89), (3, '英语', 95),
        (4, '语文', 85), (4, '数学', 76), (4, '英语', 99)
    ])

    def process(data):
        id = data[0]
        name = ''
        for s in stu_info_list:
            if s[0] == id:
                name = s[1]

        return (name, data[1], data[2])


    print(score_info_rdd.map(process).collect())

    # 有什么问题：
    # 1. stu_info_list 是在driver端运行的， 后面的rdd是在executor端运行的，需要县 序列化 再通过网络传输
    # 2. 假如一个executor上运行了2个分区， 每个分区都会收到一份stu_info_list， 但是实际上一个executor上只需要一份就够了， 分区间可以共享stu_info_list
    # 如何解决： 使用广播变量
    # 当一个变量被标记为广播变量，每个executor进程只会收到一份，节省网络传输和内存
