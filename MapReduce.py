import os

def add_tuple(key, value, d):
    if key in d.keys():
        d[key].append(value)
    else:
        d[key] = [value]

class MapReduce:

    def _default_partitioner(self, key, value):
        return hash(key) % self._num_reducer


    def __init__(self, mapper, reducer, combiner=None, partitioner=None, num_reducer=1):
        self._mapper = mapper
        self._reducer = reducer
        self._combiner = combiner
        if partitioner == None:
            partitioner = self._default_partitioner
        self._partitioner = partitioner
        self._num_reducer = num_reducer

    def run(self, filein, outdir):
        line_number = 0
        tuples = {}

        # MAPPER PHASE (mapper + combiner + partitioner)
        with open(filein, 'r') as infile:
            for value in infile:
                value = value.strip()
                mapper_tuples = {}
                for (new_key, new_value) in self._mapper(line_number, value):
                    add_tuple(new_key, new_value, mapper_tuples)

                if not self._combiner is None:
                    after_combiner_tuples = {}
                    for key in mapper_tuples.keys():
                        for (new_key, new_value) in self._combiner(key, mapper_tuples[key]):
                            add_tuple(new_key, new_value, after_combiner_tuples)

                    mapper_tuples = after_combiner_tuples

                for key in mapper_tuples.keys():
                    for value in mapper_tuples[key]:
                        idx = self._partitioner(key, value)
                        print("idx:", idx, "key:", key, "num_reducer:", self._num_reducer)
                        if idx in tuples.keys():
                            if key in tuples[idx].keys():
                                tuples[idx][key].append(value)
                            else:
                                tuples[idx][key] = [value]
                        else:
                            tuples[idx] = {}
                            tuples[idx][key] = [value]

            line_number += 1

        # REDUCER PHASE
        if not os.path.exists("./" + str(outdir)):
            os.makedirs("./" + str(outdir))
        for group in tuples.keys():
            with open("./" + str(outdir) + "/r-{0:05}".format(group), 'w') as outfile:
                for key in tuples[group].keys():
                    for t in self._reducer(key, tuples[group][key]):
                        outfile.write(str(t[0]) + "\t" + str(t[1]))
                        outfile.write("\n")
