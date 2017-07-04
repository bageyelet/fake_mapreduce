# fake_mapreduce
A python fake mapreduce

example of use:
```python
def mapper(key, value):
    words = value.split()
    for word in words:
        yield (word.lower(), 1)

def reducer(key, l):
    s = 0
    for el in l:
        s += el
    yield (key, s)

mapred = MapReduce(mapper, reducer, combiner=reducer)
mapred.run("in.txt", "out")
```

| in.txt        | out/r-00000   |
| ------------- |:-------------:|
| The cat is on the table
And the fox hit the cat
With a blue hammer on
the table dog cat blue | 2 |