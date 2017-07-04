import sys
sys.path.insert(0, '../')
from MapReduce import *

def mapper(key, value):
    words = value.split()
    for word in words:
        yield (word.lower(), 1)

def reducer(key, l):
    s = 0
    for el in l:
        s += el
    yield (key, s)

mapred = MapReduce(mapper, reducer, combiner=reducer, num_reducer=2)
mapred.run("in.txt", "out")