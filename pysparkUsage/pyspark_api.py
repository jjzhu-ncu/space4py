__author__ = 'jjzhu'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
try:
    # 引用mysql连接模块
    from mysqlconn import MysqlConnection
    # from PySparkUsage.util.logger import Logger
except ImportError:  # 如果未能成功导入
    import sys
    import os
    # 将当前的项目路径添加到系统路径变量中
    # 这里是/home/hadoop/jjzhu/workspace
    # 如果改变了的话，也要相应的改变这里的路径
    sys.path.append(os.path.abspath('../'))
    from PySparkUsage.mysqlconn import MysqlConnection
from pyspark.sql import DataFrameWriter


class SparkAPI():
    
    def __init__(self):
            self.conf = (SparkConf()
                         .setAppName("Data Analysis")
                         # 指定mysql 驱动jar包
                         .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
                         .set('spark.driver.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
                         )
            self.sc = SparkContext(conf=self.conf)
            self.sql_context = SQLContext(self.sc)
            # 批量插入
            self.BATCH_SIZE = 1000
    
            # 链接到数据库
            #  self.mysqlconn = MysqlConnection(db='core', host='10.9.29.212', passwd='')
    
    def _map(self):
        x = self.sc.parallelize([1, 2, 3])
    
        def my_func(e):
            return e, e*100, e**2
        y = x.map(my_func)
    
        print(x.collect())
        print(y.collect())

    def flat_map(self):
        x = self.sc.parallelize([1, 2, 3])
        y = x.flatMap(lambda e: (e, e**2))
        print(x.collect())
        print(y.collect())

    def map_partitions(self):
        x = self.sc.parallelize([1, 2, 3, 4], 2)  # 划分为两个分区
    
        def my_func(iterator):
            yield min(iterator)
        y = x.mapPartitions(my_func)
        print(x.glom().collect())
        print(y.glom().collect())
    
    def map_values(self):
        a = self.sc.parallelize(["dog", "tiger", "lion", "cat", "panther", " eagle"])
        b = a.map(lambda elem: (elem.__len__(), elem))
        print(b.mapValues(lambda v: 'v-'+v+'-v').collect())
    
    def flat_map_values(self):
        a = self.sc.parallelize([(1, 2), (3, 4), (5, 6)])
        b = a.flatMapValues(lambda v: v+1)
        print(b.collect())
    
    def _filter(self):
        x = self.sc.parallelize([i for i in range(10)])
        y = x.filter(lambda e: e % 2 == 1)  # 取出所有奇数
        print(x.collect())
        print(y.collect())
    
    def _distinct(self):
        x = self.sc.parallelize([1, 2, 1, 3, 4, 2, 3])
        y = x.distinct()
        print(x.collect())  # [1, 2, 1, 3, 4, 2, 3]
        print(y.collect())  # [2, 4, 1, 3]
    
    def _sample(self):
        x = self.sc.parallelize(range(10))
        # 随机取样五次，每个样本被选择的概率为0.5
        y_list = [x.sample(False, 0.5) for i in range(5)]
        print(x.collect())
        for cnt, y in zip(range(len(y_list)), y_list):
            print('sample:' + str(cnt) + ' y = ' + str(y.collect()))
    
    def take_sample(self):
        x = self.sc.parallelize(range(10))
        #  采样五次，每次随机选5个样本
        y_list = [x.takeSample(False, 5) for i in range(5)]
        print(x.collect())
        for cnt, y in zip(range(len(y_list)), y_list):
            print('sample:' + str(cnt) + ' y = ' + str(y))

    def _union(self):
        x = self.sc.parallelize(['a', 'b', 'c'])
        y = self.sc.parallelize(['A', 'B', 'C'])
        z = x.union(y)
        print(x.collect())  # ['a', 'b', 'c']
        print(y.collect())  # ['A', 'B', 'C']
        print(z.collect())  # ['a', 'b', 'c', 'A', 'B', 'C']

    def _intersection(self):
        x = self.sc.parallelize(['a', 'b', 'c'])
        y = self.sc.parallelize(['a', 'B', 'c'])
        z = x.intersection(y)
        print(x.collect())  # ['a', 'b', 'c']
        print(y.collect())  # ['A', 'B', 'C']
        print(z.collect())  # ['a', 'c']

    def sort_by_key(self):
        x = self.sc.parallelize([('Lucy', 10), ('Andy', 20), ('John', 15)])
    
        def my_func(e):
            print(e)
            return e
        y = x.sortBy(ascending=False, keyfunc=my_func)  # [('Lucy', 10), ('John', 15), ('Andy', 20)]
        print(y.collect())

    def sort_by(self):
        x = x = self.sc.parallelize([('Lucy', 10), ('Andy', 20), ('John', 15)])
    
        y = x.sortBy(keyfunc=lambda e: e[1])
        print(y.collect())

    def _cartesian(self):
        x = self.sc.parallelize(['a', 'b'])
        y = self.sc.parallelize(['c', 'd'])
        z = x.cartesian(y)  # [('a', 'c'), ('a', 'd'), ('b', 'c'), ('b', 'd')]
        print(z.collect())

    def group_by(self):
        x = self.sc.parallelize(range(10))
        y = x.groupBy(f=lambda e: 'odd' if e % 2 == 1 else 'even')
        #  [('even', [0, 2, 4, 6, 8]), ('odd', [1, 3, 5, 7, 9])]
        print([(j[0], [i for i in j[1]]) for j in y.collect()])

    def _reduce(self):
        """
        x = self.sc.parallelize(range(101))
        y = x.reduce(lambda a, b: a+b)  # sum
        print(y)
        """
        x = self.sc.parallelize([(1, 2), (1, 3), (2, 1), (2, 2)])
    
        def _reduce1(a, b):
            if a is None and b is not None:
                return b
            if a is not None and b is None:
                return a
            if a[0] == b[0]:
                return a[0], a[1] + b[1]
        y = x.reduce(_reduce1)
        print(y.collect())

    def reduce_by_key(self):
        """
        # 简单的wordcount
        sentence = ['I like hadoop',
                    'hadoop is a framework of distributed computing',
                    'I am learning Spark',
                    'Spark is ']
        x = self.sc.parallelize(sentence)
        y = x.flatMap(lambda e: e.split(' ')).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
        for word, count in y.sortBy(aself.scending=False, keyfunc=lambda e: e[1]).collect():
            print((word, count))
        """
        x = self.sc.parallelize([(1, {'1-1': 1, '1-2': 2, '2-1': 2, '2-2': 4}), (2, {'1-1': 1, '1-2': 2, '2-1': 2, '2-2': 4})])

    def _fold(self):
        x = self.sc.parallelize([1, 2, 3, 4, 5], 1)
        # print(self.sc.defaultMinPartitions)
        normal_no_zero = 10  # 如果是*，赋值为1
        y = x.fold(normal_no_zero, lambda obj, accumulated: obj + accumulated)
        print(y)  # 5050

    def _aggregate(self):
        x = self.sc.parallelize([1, 2, 3, 4])
        normal_no_zero = (0, 1)
        seq_op = (lambda aggregated, el: (aggregated[0] + el, aggregated[1]*el))
        com_op = (lambda aggregated, el: (aggregated[0] + el[0], aggregated[1]*el[1]))
        y = x.aggregate(normal_no_zero, seq_op, com_op)
        print(y)

    def _histogram(self):
        x = self.sc.parallelize([1, 3, 1, 2, 3])
        y = x.histogram(buckets=2)
        print(y)  # ([1, 2, 3], [2, 3])
        y = x.histogram([1, 2, 3, 4, 5])
        print(y)  # ([1, 2, 3, 4, 5], [2, 1, 2, 0])

    def count_by_value(self):
        x = self.sc.parallelize([1, 3, 1, 2, 3, 'a', 'a'])
        y = x.countByValue()
        print(x.collect())
        print(y)  # {'a': 2, 1: 2, 2: 1, 3: 2}

    def _top(self):
        x = self.sc.parallelize([1, 9, 12, 2, 5, 6, 78, 8])
        y = x.top(3)  # [78, 12, 9]
        print(y)

    def take_ordered(self):
        x = self.sc.parallelize([1, 2, 4, 1, 3])
        y = x.takeOrdered(num=3)  # [1, 1, 2]
        print(y)
        x = self.sc.parallelize([('Lucy', 4), ('Andy', 5), ('John', 1)])
        y = x.takeOrdered(num=2, key=lambda e: e[1])  # [('John', 1), ('Lucy', 4)]
        print(y)
    
    def collect_as_map(self):
        x = self.sc.parallelize([('C', 3), ('A', 1), ('B', 2)])
        y = x.collectAsMap()  # {'B': 2, 'A': 1, 'C': 3}
        print(y)
    
    def _join(self):
        x = self.sc.parallelize([('Lucy', 7), ('Andy', 2), ('John', 3)])
        y = self.sc.parallelize([('Lucy', 3), ('Lucy', 4), ('John', 9)])
        z = x.join(y)  # [('John', (3, 9)), ('Lucy', (7, 3)), ('Lucy', (7, 4))]
        print(z.collect())

    def combine_by_key(self):
        x = self.sc.parallelize([('Lucy', 7), ('Andy', 2), ('John', 3), ('Liko', 9)])

    def for_each(self):
        x = self.sc.parallelize([1, 2, 3,  4], 3)
        file_name = './foreachExample.txt'
    
        def f(el):
            # 追加
            f1 = open(file_name, 'a+')
            print(el, file=f1)
        # 清空/创建文件内容
        open(file_name, 'w').close()
        y = x.foreach(f)
        print(x.collect())
        print(y)  # foreach returns 'None'
        with open(file_name, 'r') as foreach_exp:
            print(foreach_exp.read())

    def foreach_partition(self):

        x = self.sc.parallelize([1, 2, 3, 4, 5])
        file_name = './0012foreachPartitionExample.txt'
        print(self.sc.defaultMinPartitions)
    
        def f(partition):
            fl = open(file_name, 'a+')
            print([el for el in partition], file=fl)
        open(file_name, 'w').close()
        y = x.foreachPartition(f)
        print(x.glom().collect())
        print(y)
        with open(file_name, 'r') as foreach_pa:
            print(foreach_pa.read())
        """
        from pyspark.sql import Row
        line = self.sc.textFile('hdfs://master:9000/jjzhu/forEachPartion.txt')
        data = line.map(lambda le: le.split(' '))\
            .map(lambda info: Row(name=info[1], score=float(info[0]), address=info[2]))
        data_df = self.sql_context.createDataFrame(data)

        def partition_insert(partition):
            import sys
            import os
            sys.path.append(os.path.abspath('../'))
            from PySparkUsage.mysqlconn import MysqlConnection
            insert_query = 'insert into test(score, name, address) value (%s, %s, %s)'
            conn = MysqlConnection(db='core', host='10.9.29.212', passwd='')
            for row in partition:
                row_dic = row.asDict()

                conn.execute_single(insert_query, (row_dic['score'], row_dic['name'], row_dic['address']))
            conn.close()
        # data_df.foreachPartition(partition_insert)

        writer = DataFrameWriter(data_df)
        writer.jdbc("jdbc:mysql://10.9.29.212:3306/core?user=root&characterEncoding=UTF-8",
                    table='test_c',
                    mode='append')
        """

    def load_from_mysql(self, db, dbtable):
        """
        通过指定mysql将数据库中的表加载为DataFrame
        :param db: 数据库名
        :param dbtable: 表名
        :return: DataFrame
        """
        url = "jdbc:mysql://10.9.29.212:3306/"+db+"?user=root&characterEncoding=UTF-8"
        df = self.sql_context.read.format("jdbc").options(url=url,
                                                          dbtable=dbtable,
                                                          driver="com.mysql.jdbc.Driver").load()
        # df = sqlContext.read.jdbc(url, dbtable, properties=properties)
        return df

    def insert_to_table(self):
        test = self.load_from_mysql('core', 'test')
        test_c = self.load_from_mysql('core', 'test_c')
        test.registerTempTable('test')
        writer = DataFrameWriter(test_c)
        for row in self.sql_context.sql('select * from test').collect():
            print(row.asDict())
        writer.insertInto('test')
        writer.parquet()
        for row in self.sql_context.sql('select * from test').collect():
            print(row.asDict())
        al = self.sql_context.sql('select * from test')
        al.save()
        al.write.csv('hdfs://master:9000/jjzhu/')

    def test(self):
        from pyspark.sql.types import Row

        df = self.sql_context.createDataFrame([(Row(name='Alice', age=5, height=80),
                                                Row(name='Alice', age=5, height=80),
                                                Row(name='Alice', age=10, height=80))])
        wt = DataFrameWriter(df)
        # wt.parquet('./temp/data')
        from glob import glob
        r = self.sc.parallelize([(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6 , 7)])
        r.saveAsTextFile('./temp/date2/d.data')
        print(''.join(sorted(input(glob('./temp/date2' + "/part-0000*")))))
        # r.saveAsHadoopFile()

    def read_test(self):
        from glob import glob
        # print(''.join(sorted(input(glob('./temp/date2' + "/part-0000*")))))

        print(self.sc.textFile('./temp/date2/part*').collect())
if __name__ == '__main__':
    api = SparkAPI()
    api._reduce()