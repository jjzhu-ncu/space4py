__author__ = 'jjzhu'
from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating, ALS

from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
import numpy as np
sc = SparkContext(appName='cluster')


def als():
    movies = sc.textFile('hdfs://master:9000/jjzhu/ml-100k/u.item')
    genres = sc.parallelize([
        'unknon|0', 'Action|1', 'Adventure|2', 'Animation|3', 'Children\'s|4', 'Comedy|5', 'Crime|6',
        'Documentary|7', 'Drama|8', 'Fantasy|9', 'Film-Noir|10', 'Horror|11', 'Musical|12', 'Mystery|13',
        'Romance|14', 'Sci-Fi|15', 'Thriller|16', 'War|17', 'Western|18'
    ])
    # genres = sc.textFile('./data/ml-100k/u.genre.txt')
    raw_data = sc.textFile('hdfs://master:9000/jjzhu/ml-100k/u.data')
    # 电影类别映射 1->unknown
    genre_map = genres.filter(lambda line: line.strip().__len__() != 0)\
        .map(lambda line: line.split('|')).map(lambda line: [int(line[1]), line[0]]).collectAsMap()

    # 根据类别编号获取类别
    def get_genres(genre_code):
        movie_genres = []
        for i in range(len(genre_code)):
            if int(genre_code[i]) == 1:
                movie_genres.append(genre_map[i])
        return movie_genres

    # 计算余弦相似度
    def cosine_similarity(vec1, vec2):
        return vec1.dot(vec2)/(np.linalg.norm(vec1) * np.linalg.norm(vec2))
    movies_rdd = movies.map(lambda line: line.split('|'))
    movies_rdd.cache()
    # (电影id, (电影名称, (电影分类1, 2, 3)))
    titles_and_genres = movies_rdd.map(lambda line: (int(line[0]), (line[1], (get_genres(line[5:])))))
    # （用户id， 电影id，评分）不要时间戳
    ratings = raw_data.map(lambda line: line.split('\t')[:3])\
        .map(lambda line: Rating(int(line[0]), int(line[1]), float(line[2])))
    ratings.cache()

    # 训练模型
    als_model = ALS.train(ratings, 60, 10, 0.1)
    # 产品因子
    movie_factors = als_model.productFeatures().map(lambda elem: (elem[0], Vectors.dense(elem[1])))
    movie_vectors = movie_factors.map(lambda elem: elem[1])
    # print(movie_vectors.take(1))
    # print(len(movie_vectors.take(1)[0]))
    # 用户因子
    user_factors = als_model.userFeatures().map(lambda elem: (elem[0], Vectors.dense(elem[1])))
    user_vectors = user_factors.map(lambda elem: elem[1])
    '''
    # print(als_model.predict(789, 123)) # 预测用户789对电影123的评分
    # for i in als_model.recommendProducts(789, 10):  # 为用户789推荐10部电影
    #    print(i)
    # 用户567的因子
    item_factor = np.array(als_model.productFeatures().lookup(567)[0])
    # 计算其他电影与用户567的相似度
    sims = als_model.productFeatures().map(lambda elem: (elem[0], cosine_similarity(np.array(elem[1]), item_factor)))
    # 降序排列
    top = sims.sortBy(lambda elem: elem[1], ascending=False)
    print(top.take(1)[0])
    top_10 = sc.parallelize(top.take(10))
    print(top_10.collect())
    # print(movies_rdd.map(lambda elem: (elem[0], elem[1])).collect())
    cmd_movies = top_10.join(titles_and_genres).map(lambda elem: elem[1][1])
    for m in cmd_movies.collect():
        print(m)
    '''
    movies_for_user = ratings.keyBy(lambda elem: elem[0]).lookup(789)
    print(movies_for_user)

    # print(len(user_vectors.take(1)[0]))
# print(titles_and_genres.collect())


def hdfs_word_count():
    ssc = StreamingContext(sc, 1)
    lines = ssc.textFileStream('hdfs://master:9000/jjzhu/streaming/wordcount/')
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


def streaming_k_mean():
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.regression import LabeledPoint
    ssc = StreamingContext(sc, 1)

    training_data = ssc.textFileStream('hdfs://master:9000/jjzhu/streaming/kmeans/input')\
        .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))
    test_data = ssc.textFileStream('hdfs://master:9000/jjzhu/streaming/kmeans/test')\
        .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))
    model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
    model.trainOn(training_data)
    print(model.latestModel().centers)
    result = model.predictOn(test_data)
    result.pprint()
    ssc.start()
    ssc.awaitTermination()


def transformations():
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint('hdfs://master:9000/jjzhu/streaming/wordcount/')
    lines = ssc.textFileStream('hdfs://master:9000/jjzhu/streaming/wordcount/')
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))
    r = counts.reduceByKeyAndWindow(lambda x, y: x+y, lambda x, y: x-y, 30)

    r.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    transformations()