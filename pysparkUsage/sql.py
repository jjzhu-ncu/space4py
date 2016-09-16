__author__ = 'jjzhu'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
conf = (SparkConf()
        .setAppName("load_data_from_mysql")
        .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
        )
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def create_df():
    # tup
    tup = [('Alice', 1), ['lucy', 18]]
    print('rdd of tuple')
    print(sqlContext.createDataFrame(tup).collect())
    print(sqlContext.createDataFrame(tup, ['name', 'age']).collect())
    # dict
    print('rdd of dict')
    dic = [{'name': 'Alice', 'age': 21}, {'name': 'lucy', 'age': 18}]
    print(sqlContext.createDataFrame(dic).collect())
    # Row
    from pyspark.sql import Row
    print('rdd of Row')
    rdd = sc.parallelize(tup)
    Person = Row('name', 'age')
    person = rdd.map(lambda r: Person(*r))
    print(sqlContext.createDataFrame(person).collect())


def test_sql_conn():
    # 数据库的url
    url = "jdbc:mysql://10.9.29.212:3306/core?user=root&characterEncoding=UTF-8"
    account_list_df = sqlContext.read.format("jdbc")\
        .options(url=url,
                 dbtable='t_CMMS_ACCOUNT_LIST',  # 要加载的表
                 driver="com.mysql.jdbc.Driver"  # 指定jdbc驱动
                 ).load()
    account_list_df.cache()  # 缓存
    # print(account_list_df.count())
    #  account_list_df.filter(u'ACC_NAM like "郑%"').show(10)
    # account_list_df.select('ACC_NAM', 'ACC_NO22', 'CUST_NO').show()
    # 降序排序
    # account_list_df.sort('ACC_NO22', asceding=False).show()
    account_list_df.registerTempTable('account')
    # sqlContext.sql('select ACC_NAM, ACC_NO22, CUST_NO from account').show(10)
    # sqlContext.sql('select sum(AMT) as sum, avg(AMT) as avg, max(AMT) as max, min(AMT) as min from account').show()
    account_list_df.describe('AMT').show()
    sqlContext.dropTempTable('account')
if __name__ == '__main__':
    test_sql_conn()
    sc.stop()