# coding: utf-8

import jieba

if __name__ == '__main__':
    content = '我是来自浙江杭州的一个叫千岛湖的小县城，千岛湖的风景很美，欢迎大家去玩'

    print('=' * 50)
    result = jieba.cut(content, True)

    print(result)
    print(list(result))
    print(type(result))

    print('=' * 50)
    result2 = jieba.cut(content, False)
    print(list(result2))

    print('=' * 50)
    result3 = jieba.cut_for_search(content)
    print(list(result3))

    print('*' * 50)
