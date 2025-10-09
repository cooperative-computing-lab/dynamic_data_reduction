import ndcctools.taskvine as vine
from dynamic_data_reduction import DynamicDataReduction
import itertools
import getpass

if __name__ == "__main__":
    n = 12

    some_ds_pairs = list(itertools.pairwise(range(1, n + 1)))
    another_ds_pairs = list(itertools.pairwise(range(101, 2 * n + 101)))

    data = {
        "datasets": {
            "some_ds": {"pairs": some_ds_pairs, "size": len(some_ds_pairs)},
            "another_ds": {"pairs": another_ds_pairs, "size": len(another_ds_pairs)},
        },
    }

    def source_preprocess(dataset_info, **kwargs):
        for pair in dataset_info["pairs"]:
            # yielding a pair, and a chunk size of 1 (e.g., only one pair per chunk)
            yield (pair, 1)

    def source_postprocess(pair, **kwargs):
        # from every pair, generate a dask array of size 10
        import dask.array as da

        step = (pair[1] - pair[0]) / 10.0
        return da.from_array([step * i for i in range(10)])

    def processor_double_data(datum):
        import dask.array as da

        return da.sum(datum.map_blocks(lambda x: x * 2))

    def processor_triple_data(datum):
        import dask.array as da

        return da.sum(datum.map_blocks(lambda x: x * 3))

    def reducer_add_data(a, b):
        return a + b

    mgr = vine.Manager(port=[9123, 9129], name=f"{getpass.getuser()}-simple-ddr")
    ddr = DynamicDataReduction(
        mgr,
        data=data,
        source_preprocess=source_preprocess,
        source_postprocess=source_postprocess,
        processors={"double": processor_double_data, "triple": processor_triple_data},
        accumulator=reducer_add_data,
        accumulation_size=10,
    )

    workers = vine.Factory("local", manager=mgr)
    workers.max_workers = 2
    workers.min_workers = 0
    workers.cores = 4
    workers.memory = 2000
    workers.disk = 8000
    with workers:
        result = ddr.compute()

    print(result)
