#! /usr/bin/env python

import dask
import dask.delayed


@dask.delayed
def my_map_step1(x):
    return -x


@dask.delayed
def my_map_step2(x):
    return x ** 2


@dask.delayed
def my_reduce(xs):
    return sum(xs)


cs = []
with dask.annotate(category="map"):
    for data in range(10):
        with dask.annotate(data=data):
            s1 = my_map_step1(data)
            s2 = my_map_step2(s1)
            cs.append(s2)

with dask.annotate(category="reduce"):
    result = my_reduce(cs)

dsk = result.dask
print(list(dsk.keys()))

for k, v in dsk.layers.items():
    print(k, v.annotations)

print(result.compute())
