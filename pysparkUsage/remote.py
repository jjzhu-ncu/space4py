__author__ = 'Administrator'
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

import pydevd
import os
# os.environ["SPARK_CLASSPATH"] = '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'
# pydevd.settrace('60.191.25.130', port=8617, stdoutToServer=True, stderrToServer=True)
conf = (SparkConf()
        .setAppName("load_data_from_mysql")
        # .set("spark.executor.cores", '2')
        .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
        )

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def load_from_mysql(db, dbtable):
    url = "jdbc:mysql://10.9.29.212:3306/"+db+"?user=root&password=188Lanxi-1&characterEncoding=UTF-8"
    df = sqlContext.read.format("jdbc").options(url=url, dbtable=dbtable, driver="com.mysql.jdbc.Driver").load()
    # df = sqlContext.read.jdbc(url, dbtable, properties=properties)
    return df


def save_hdfs():
    data = sc.parallelize([1, 2, 3, 4])
    data.saveAsTextFile('hdfs://master:9000/jjzhu/file.txt')


def get_time_slot(start, end, format_):
    import calendar as cal
    import datetime
    time_slot = []
    s = datetime.datetime.strptime(start, format_)
    year = s.year
    month = s.month
    e = datetime.datetime.strptime(end, format_)
    while year <= e.year and (year <= e.year or month <= e.month):
        day_range = cal.monthrange(year, month)
        time_slot.append((
            datetime.datetime(year, month, 1).strftime('%Y%m%d%H%M%S'),
            datetime.datetime(year, month, day_range[1], 23, 59, 59).strftime('%Y%m%d%H%M%S'))
        )
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return time_slot


def analysis():
    """
    data_frame = load_from_mysql('core', 'CMCADI')
    data_frame.registerTempTable('balance')

    result = data_frame.select('AB01AC15', 'AB05CSNO', 'AB16BAL').sort('AB16BAL', ascending=False).take(100)
    for i in result:
        print(i)
    """
    data_frame = load_from_mysql('core', 'BDFMHQAA_D')
    data_frame.registerTempTable('business')
    gd = data_frame.select('AA03CSNO', 'AA08PRON')

    def merge_count(a, b):
        r = {}
        for p, c in a.items():
            if p in r:
                r[p] += c
            else:
                r[p] = c
        for p, c in b.items():
            if p in r:
                r[p] += c
            else:
                r[p] = c
        return r
    result = gd.map(lambda row: (row.AA03CSNO, {row.AA08PRON: 1})).reduceByKey(merge_count)
    pron_count = gd.map(lambda row: (row.AA08PRON, 1)).reduceByKey(lambda a, b: a + b)

    # result = gd.map(lambda row: (row.AA03CSNO, row.AA08PRON))
    print(result.take(10))
    print('----------------pron count-----------------')
    print(pron_count.collect())

    print(gd)


def pay():
    start_time = '2015-03'
    end_time = '2016-04'
    time_format = '%Y-%m'
    time_slot = get_time_slot(start_time, end_time, time_format)
    print(time_slot)
    df = load_from_mysql('data', 'YS_PAY_REAL')
    df.registerTempTable('pay_info')
    filter_df = df.select('CARDNO', 'USERNAME', 'AMOUNT', 'TRANSACTIONDATE', 'RETURNCODE')\
        .filter(df.TRANSACTIONDATE >= time_slot[1][0]).filter(df.TRANSACTIONDATE <= time_slot[time_slot.__len__()-1][1])\
        .filter(df.RETURNCODE == '00')
    trs = filter_df.groupBy('CARDNO').count().sort('count', ascending=False).take(5)

    result = []
    for tr in trs:
        card_no = tr.asDict()['CARDNO']
        print(card_no)
        for slot in time_slot:
            print(slot[0]+u"->"+slot[1])
            df3 = filter_df.filter(filter_df.CARDNO == card_no).filter(filter_df.TRANSACTIONDATE <= slot[1])\
                .filter(filter_df.TRANSACTIONDATE >= slot[0])\
                .groupBy().sum('AMOUNT')
            print(df3.collect())


def test_blog():

    df = load_from_mysql('blog', '')
    print(df.columns)
    df.registerTempTable('blog')

    # df2 = sqlContext.sql('select CARDNO, USERNAME, AMOUNT, TRANSACTIONDATE, RETURNCODE from user')
    df2 = df.select('blog_text', 'blog_source', 'blog_created_at', 'blog_user_id').filter()
    rows = df2.groupBy('blog_user_id').count().sort('count', ascending=False).take(100)

    for row in rows:
        user_id = row.asDict()['blog_user_id']
        df3 = df2.filter(df2.blog_user_id == user_id).filter(df2.RETURNCODE == '00')
        df3.show()
    print(rows)

if __name__ == '__main__':
    analysis()
    sc.stop()