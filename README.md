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

in.txt
```
The cat is on the table
And the fox hit the cat
With a blue hammer on
the table dog cat blue
```
out/r-00000
```
the 5
on  2
table   2
dog 1
and 1
a   1
hammer  1
blue    2
fox 1
hit 1
cat 3
is  1
with    1
```