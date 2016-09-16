__author__ = 'jjzhu'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.mllib.clustering import KMeans, KMeansModel, GaussianMixture, BisectingKMeans
conf = (SparkConf()
        .setAppName("kmeans test")
        .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
        )
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
from numpy import array
training_data = sc.textFile('../data/Wholesale_customers_data.csv')

process_data = training_data.filter(lambda line: line.find('Channel') < 0)\
    .map(lambda line: array([float(i) for i in line.split(',')]))
process_data.cache()


def kmeans_test():
    file = open('../kmeans_result.txt', mode='wt', encoding='utf-8')
    content = []
    for k in range(1, 15):
        model = KMeans.train(process_data, k)
        cost = model.computeCost(process_data)

        content.append('sum of squared distances of points to their nearest center when k=%s -> %s \n' % (str(k), str(cost)))
        content.append(str(model.clusterCenters)+"\n")
    file.writelines(content)
    file.close()


def gaussian_mixture():
    """
    混合高斯模型
    :return:
    """
    gm_model = GaussianMixture.train(process_data, 9)
    gm_model



if __name__ == '__main__':
    kmeans_test()