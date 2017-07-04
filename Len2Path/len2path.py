import sys
sys.path.insert(0, '../')
from MapReduce import *

def mapperPhase1(key, value):
    edge = value.split()
    yield (edge[0], "o " + str(edge[1]))
    yield (edge[1], "i " + str(edge[0]))

def reducerPhase1(key, l):
    in_set = []
    out_set = []

    for el in l:
        token = el.split()
        if token[0] == "o":
            out_set.append(token[1])
        elif token[0] == "i":
            in_set.append(token[1])
        else:
            raise Exception
    
    for i in in_set:
        for o in out_set:
            yield (i, o)

def mapperPhase2(key, value):
    yield (value, "")

def reducerPhase2(key, l):
    yield (key, "")

mapred1 = MapReduce(mapperPhase1, reducerPhase1)
mapred1.run("in.txt", "tmp")
mapred2 = MapReduce(mapperPhase2, reducerPhase2)
mapred2.run("tmp/r-00000", "out")