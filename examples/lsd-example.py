# run using:
# spark-janelia -n 3 lsd -s test_spark_batchmode.py
# runs with 3 nodes
from pyspark import SparkConf, SparkContext

def square(x):
    return x*x

conf = SparkConf().setAppName('test_spark_batchmode')
sc = SparkContext(conf=conf)
data = range(10)
datasquared = sc.parallelize(data, len(data)).map(square).collect()
print(zip(data,datasquared))
