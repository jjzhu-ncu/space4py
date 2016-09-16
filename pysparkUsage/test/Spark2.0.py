__author__ = 'jjzhu'
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .appName('spark 2.0 sql test')\
    .getOrCreate()