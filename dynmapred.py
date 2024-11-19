#! /usr/bin/env python

import dask
import dask.array as da

from functools import reduce
import cloudpickle
import lz4.frame
import sys

from collections import defaultdict

import ndcctools.taskvine as vine


def wrap_processing(processor, source_connector, datum, local_executor="threads", local_executor_args=None):
    import os

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    datum_post = source_connector(datum)

    result = processor(datum_post).compute(executor=local_executor, **local_executor_args)
    with lz4.frame.open("proc_out.p", "wb") as fp:
        cloudpickle.dump(result, fp)
    return result


def accumulate(accumulator, result_names, to_file=True):
    out = None

    for r in sorted(result_names):
        with lz4.frame.open(r, "rb") as fp:
            other = cloudpickle.load(fp)

        if out is None:
            out = other
        else:
            out = accumulator(out, other)
            del other

    if to_file:
        with lz4.frame.open("accum_out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


def accumulate_tree(accumulator, result_names, to_file=True, local_executor='threads', local_executor_args=None):
    import dask
    import os

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    def load_file(filename):
        with lz4.frame.open(r, "rb") as fp:
            return cloudpickle.load(fp)

    to_reduce = []
    task_graph = {}
    for r in sorted(result_names):
        key = ("load", len(task_graph))
        task_graph[key] = (load_file, r)
        to_reduce.append(key)

    while len(to_reduce) > 1:
        key = ("merge", len(task_graph))
        firsts, to_reduce = to_reduce[:2], to_reduce[2:]
        task_graph[key] = (accumulator, *firsts)
        to_reduce.append(key)

    print(task_graph)

    out = dask.get(task_graph, to_reduce[0], executor=local_executor, **local_executor_args)
    if to_file:
        with lz4.frame.open("accum_out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


class DynMapReduce:
    def __init__(self, manager, source_connector, processor, accumulator):
        self._manager = manager
        self._source_connector = source_connector
        self._processor = processor
        self._accumulator = accumulator

        self._max_active = 10
        self._task_active = set()

        self._id_to_output = {}

        self._accumulation_size = 30

        self._last_accum_id = None

        self._ds_done_count = defaultdict(int)
        self._ds_active_count = defaultdict(int)
        self._ds_all_submitted = defaultdict(lambda: False)
        self._ds_to_accumulate = defaultdict(lambda: [])
        self._ds_outputs = defaultdict(lambda: None)

        self._set_env()

    def _set_env(self, env="env.tar.gz"):
        functions = [wrap_processing, accumulate, accumulate_tree]
        # if self.lib_extra_functions:
        #     functions.extend(self.lib_extra_functions)
        self._lib_name = f"dynmapred-{id(self)}"
        libtask = self.manager.create_library_from_functions(
            self._lib_name,
            *functions,
            poncho_env="dummy-value",
            add_env=False,
            init_command=None,
            hoisting_modules=None
        )
        envf = self.manager.declare_poncho(env)
        libtask.add_environment(envf)
        self.manager.install_library(libtask)

        self._env = envf
        self._dynmapred_file = self.manager.declare_file(__file__, cache=True)
        self._coffea_dir = self.manager.declare_file("coffea", cache=True)

    @property
    def manager(self):
        return self._manager

    def _add_fetch_task(self, result_task):
        task = vine.Task("cp in out")

        ds = result_task.metadata["dataset"]

        result = self.manager.declare_file(f"{self.manager.staging_directory}/output_{result_task.id}", cache=False)
        task.add_input(self._id_to_output[result_task.id], "in")

        task.add_output(result, "out")

        task.set_category(f"fetch#{ds}")
        task.metadata = {"type": "fetch", "dataset": ds}
        self._ds_outputs[ds] = result
        self.submit(task)

    def _add_proc_task(self, datum, dataset, local_executor='threads', local_executor_args=None):
        result_file = self.manager.declare_temp()
        #task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)

        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(wrap_processing, self._processor, self._source_connector, datum)
        # task.add_environment(self._env)

        ds = dataset
        task.set_category(f"processing#{ds}")
        task.metadata = {"type": "processing", "dataset": ds}

        task.set_cores(1)
        task.add_output(result_file, "proc_out.p")
        self._id_to_output[self.submit(task)] = result_file

    def _add_accum_tasks(self, task, temp_result=True):
        cat, ds = task.category.split("#")

        accum_size = max(2, self._accumulation_size)
        if self._ds_all_submitted[ds]:
            if self._ds_active_count[ds] == 0:
                if len(self._ds_to_accumulate[ds]) == 1:
                    last = self._ds_to_accumulate[ds].pop()
                    if last.metadata["type"] != "fetch":
                        self._add_fetch_task(last)
                    return

                accum_size = min(accum_size, len(self._ds_to_accumulate[ds]))

        if len(self._ds_to_accumulate[ds]) < accum_size:
               return

        firsts, self._ds_to_accumulate[ds] = (
            self._ds_to_accumulate[ds][:accum_size],
            self._ds_to_accumulate[ds][accum_size:]
        )

        # task = vine.FunctionCall(self._lib_name, "accumulate", self._accumulator, [f"input_{tid}" for tid in firsts])
        task = vine.PythonTask(
            # accumulate_tree,
            accumulate,
            self._accumulator,
            [f"input_{t.id}" for t in firsts],
        )

        result_file = self.manager.declare_temp()
        task.add_output(result_file, "accum_out.p")

        for t in firsts:
            task.add_input(self._id_to_output[t.id], f"input_{t.id}")

        task.set_category(f"accumulating#{ds}")
        task.metadata = {"type": "accumulating", "dataset": ds}
        self._id_to_output[self.submit(task)] = result_file

    def wait(self, timeout):
        t = self.manager.wait(5)
        if t:
            print(f"task {t.id} '{t.category}' done: {t.result} {t.std_output}")
            if t.successful():
                ds = t.metadata["dataset"]
                self._ds_active_count[ds] -= 1
                self._ds_done_count[ds] += 1
                self._ds_to_accumulate[ds].append(t)
            else:
                raise RuntimeError(f"task could not be completed {t.output}")
        return t

    def submit(self, task):
        task.add_input(self._dynmapred_file, "dynmapred.py")
        task.add_input(self._coffea_dir, "coffea")

        ds = task.metadata["dataset"]
        self._ds_active_count[ds] += 1
        tid = self.manager.submit(task)

        return tid

    def generate_tasks(self, datasets):
        for ds, info in datasets.items():
            for datum in info:
                yield (datum, ds)
            self._ds_all_submitted[ds] = True

    def compute(
        self,
        data,
        manager_args=None,
        remote_executor=None,
        remote_executor_args=None,
    ):
        data_iter = self.generate_tasks(data)

        (datum, dataset) = next(data_iter)
        self._add_proc_task(datum, dataset)
        while not self.manager.empty():
            for (datum, dataset) in data_iter:
                if len(self._task_active) >= self._max_active:
                    break
                self._add_proc_task(datum, dataset)

            t = self.wait(5)
            if t:
                if t.successful():
                    self._add_accum_tasks(t)
                else:
                    print("-", t.output)
                    print(t.std_output)

        results = {}
        for ds in data:
            f = self._ds_outputs[ds]
            with lz4.frame.open(f"{f.source()}", "rb") as fp:
                results[ds] = cloudpickle.load(fp)

        return results


if __name__ == '__main__':
    data = (da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 1000))
#data = [da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 10000)]

    def double_data(d):
        return d.map_blocks(lambda x: x * 2)

    def add_data(a, b):
        return a + b

    mgr = vine.Manager(port=9123)
    dmr = DynMapReduce(mgr, source_connector=lambda x: x, processor=double_data, accumulator=add_data)
    result = dmr.compute(data)

# workers = vine.Factory("local", manager=mgr)
# workers.max_workers = 1
# workers.min_workers = 0
# workers.cores = 4
# workers.memory = 2000
# workers.disk = 8000
#
# with workers:
#     result = dmr.compute(data)

    print(result)



    sys.exit(0)



    c = [double_data(d).compute() for d in data]
    print(c)


