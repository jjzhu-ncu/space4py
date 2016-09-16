__author__ = 'jjzhu'

from pyspark import SparkContext
import time


def avoid_create_repeated_rdd():
    sc = SparkContext(appName='avoid_create_repeated_rdd')
    rdd = sc.textFile('./data/file1.txt')
    print(rdd.map(lambda line: line.split(' ')).collect())
    rdd1 = sc.textFile('./data/file1.txt')  # don't do like this
    # 用rdd和rdd1读的是同一个文件，内容一样
    # 用print(rdd.reduce(lambda a, b: a + b).collect())代替
    # but， 由于spark的机制，对同一个rdd执行不同的算子操作时，
    # 都会再次从头重新开始计算，所有 替换并没有完全解决一个rdd
    # 被多次计算的问题，可以将需要多次利用的rdd持久化进行改进
    print(rdd1.reduce(lambda a, b: a + b))


def possible_reuse_rdd():
    sc = SparkContext(appName='possible_reuse_rdd')
    rdd = sc.parallelize([('a', 10), ('b', 20), ('a', 5), ('b', 1), ('c', 10)])


def persist_rdd():
    sc = SparkContext(appName='persist rdd')
    # 对同一个rdd进行多次算子操作，可以将rdd持久化到内存中
    # rdd = sc.textFile('./data/file1.txt')
    # rdd.map(lambda line: line.split(','))
    # rdd.reduce(lambda a, b: a+' '+b)
    # 对rdd持久化spark提供了两种方式 cache()和persist（）
    # cache:
    #     Persist this RDD with the default storage level (C{MEMORY_ONLY_SER}).
    # rdd = sc.textFile('./data/file1.txt').cache()
    # persist:
    #   Set this rdd's storage level to persist its values across operations
    #    after the first time it is computed. This can only be used to assign
    #    a new storage level if the RDD does not have a storage level set yet.
    #    If no storage level is specified defaults to (C{MEMORY_ONLY_SER}).
    from pyspark.storagelevel import StorageLevel
    rdd = sc.textFile('./data/file1.txt').persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
    print(rdd.map(lambda line: line.split(',')).collect())
    print(rdd.reduce(lambda a, b: a+' '+b))
    rdd.reduceByKey()


# TODO
def avoid_use_shuffle():

    sc = SparkContext(appName='persist rdd')

    # 分发到两个分区上去
    x = sc.parallelize([('Lucy', 7), ('Andy', 2), ('John', 3)], 2)
    y = sc.parallelize([('Lucy', 3), ('Lucy', 4), ('John', 9)], 2)
    # 传统的join会导致shuffle操作，网络传输、频繁的IO很耗时
    start = time.time()
    z = x.join(y)
    z.countByKey()
    print(z.collect())
    end = time.time()
    print('join operation spend time :' + str(end-start)+'s')
    start = time.time()
    x2 = x.collect()
    x2db = sc.broadcast(x2)
    # def _join(a, x2db):

    # z = y.map(x2db)


def coalesce_after_filter():
    sc = SparkContext(appName='coalesce_after_filter')
    rdd = sc.parallelize([i for i in range(1 << 20)], 4)
    # 手动coalesce
    print(rdd.filter(lambda elem: elem % 2 == 0).coalesce(2).glom().collect())
if __name__ == '__main__':
    coalesce_after_filter()
