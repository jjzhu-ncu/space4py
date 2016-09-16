from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# sc = SparkContext(master='local[*]',appName='Streaming Usage')
sc = SparkContext(appName='Streaming Usage')
ssc = StreamingContext(sc, 1)