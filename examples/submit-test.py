#!/misc/local/python-2.7.11/bin/python2.7
import numpy as np
from pyspark import SparkContext
add_files = None
sc = SparkContext(pyFiles=add_files)
print np.arange(10)
data = sc.parallelize(np.arange(10))
print data.first()
print data.collect()
def square(x):
        return np.square(x)

print data.map(square).collect()

print data.map(lambda x: np.square(x)).collect()
print data.filter(lambda x: np.mod(x, 2) == 1). collect()
print data.reduce(lambda x, y: x+y)
