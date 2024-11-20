#! /usr/bin/env python

import dask.array as da
import cloudpickle
import lz4.frame
import sys
import rich
from dataclasses import dataclass

from collections import defaultdict
import ndcctools.taskvine as vine

from typing import Any, Callable, Mapping, Optional, TypeVar


def wrap_processing(
    processor,
    source_connector,
    datum,
    local_executor="threads",
    local_executor_args=None,
):
    import os

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    datum_post = source_connector(datum)

    result = processor(datum_post).compute(
        executor=local_executor, **local_executor_args
    )
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


def accumulate_tree(
    accumulator,
    results,
    to_file=True,
    from_files=True,
    local_executor="threads",
    local_executor_args=None,
):
    import dask
    import os

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    if from_files:

        def load(filename):
            with lz4.frame.open(filename, "rb") as fp:
                return cloudpickle.load(fp)

    else:

        def load(result):
            return result

    to_reduce = []
    task_graph = {}
    for r in results:
        key = ("load", len(task_graph))
        task_graph[key] = (load, r)
        to_reduce.append(key)

    while len(to_reduce) > 1:
        key = ("merge", len(task_graph))
        firsts, to_reduce = to_reduce[:2], to_reduce[2:]
        task_graph[key] = (accumulator, *firsts)
        to_reduce.append(key)

    out = dask.get(
        task_graph, to_reduce[0], executor=local_executor, **local_executor_args
    )
    if to_file:
        with lz4.frame.open("accum_out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    return out


def identity_source_conector(datum):
    return datum


def default_accumualtor(a, b):
    return a + b


P = TypeVar("P")
H = TypeVar("H")


@dataclass
class DynMapReduce:
    manager: vine.Manager
    processor: Callable[[P], H]
    source_connector: Callable[[Any], P] = identity_source_conector
    accumulator: Callable[[H, H], H] = default_accumualtor
    max_tasks_active: Optional[int] = None
    max_tasks_per_dataset: Optional[int] = None
    max_task_retries: int = 10
    accumulation_size: int = 10
    resources_processing: Optional[Mapping[str, float]] = None
    resources_accumualting: Optional[Mapping[str, float]] = None
    environment: Optional[str] = None
    x509_proxy: Optional[str] = None

    def __post_init__(self):
        self._task_active = 0
        self._all_proc_submitted = False
        self._id_to_output = {}
        self._ds_done_count = defaultdict(int)
        self._ds_active_count = defaultdict(int)
        self._ds_all_submitted = defaultdict(lambda: False)
        self._ds_to_accumulate = defaultdict(lambda: [])
        self._ds_outputs = defaultdict(lambda: None)

        if not self.resources_processing:
            self.resources_processing = {"cores": 1}

        if not self.resources_accumualting:
            self.resources_accumualting = {"cores": 1}

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
            hoisting_modules=None,
        )
        envf = self.manager.declare_poncho(env)
        libtask.add_environment(envf)
        self.manager.install_library(libtask)

        self._env = envf
        self._dynmapred_file = self.manager.declare_file(__file__, cache=True)
        self._coffea_dir = self.manager.declare_file("coffea", cache=True)

        self._proxy_file = None
        if self.x509_proxy:
            self._proxy_file = self.manager.declare_file(self.x509_proxy, cache=True)

    def _add_fetch_task(self, result_task):
        task = vine.Task("cp in out")

        ds = result_task.metadata["dataset"]

        result = self.manager.declare_file(
            f"{self.manager.staging_directory}/output_{result_task.id}", cache=False
        )
        task.add_input(self._id_to_output[result_task.id], "in")

        task.add_output(result, "out")

        task.set_cores(1)
        task.set_category(f"fetch#{ds}")
        task.metadata = {"type": "fetch", "dataset": ds}
        self._ds_outputs[ds] = result
        self.submit(task)

    def _set_resources(self, data):
        for ds in data:
            self.manager.set_category_resources_max(
                f"processing#{ds}", self.resources_processing
            )
            self.manager.set_category_resources_max(
                f"accumulating#{ds}", self.resources_accumualting
            )

    def _add_proc_task(
        self, datum, dataset, attempt=1, local_executor="threads", local_executor_args=None
    ):
        result_file = self.manager.declare_temp()

        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(
            wrap_processing, self.processor, self.source_connector, datum
        )
        if self.environment:
            task.add_environment(self.environment)

        ds = dataset
        task.set_category(f"processing#{ds}")
        task.metadata = {"type": "processing", "dataset": ds, "input": datum, "retries": attempt}
        task.add_output(result_file, "proc_out.p")
        self._id_to_output[self.submit(task)] = result_file

    def _add_accum_tasks(self, task, temp_result=True):
        cat, ds = task.category.split("#")

        accum_size = max(2, self.accumulation_size)
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
            self._ds_to_accumulate[ds][accum_size:],
        )

        # task = vine.FunctionCall(self._lib_name, "accumulate", self._accumulator, [f"input_{tid}" for tid in firsts])
        task = vine.PythonTask(
            accumulate_tree,
            #accumulate,
            self.accumulator,
            [f"input_{t.id}" for t in firsts],
        )

        result_file = self.manager.declare_temp()
        task.add_output(result_file, "accum_out.p")

        for t in firsts:
            task.add_input(self._id_to_output[t.id], f"input_{t.id}")

        task.set_cores(1)
        task.set_category(f"accumulating#{ds}")
        task.metadata = {"type": "processing", "dataset": ds}
        task.add_output(result_file, "proc_out.p")
        self._id_to_output[self.submit(task)] = result_file

    def wait(self, timeout):
        t = self.manager.wait(5)
        if t:
            print(f"task {t.id} '{t.category}' done: {t.result} status: {t.exit_code}")
            ds = t.metadata["dataset"]
            self._task_active -= 1
            self._ds_active_count[ds] -= 1
            if t.successful():
                self._ds_done_count[ds] += 1
                self._ds_to_accumulate[ds].append(t)
            else:
                if (
                    t.metadata["type"] == "processing"
                    and t.metadata["retries"] < self.max_task_retries
                ):
                    print(f"resubmitting task {t.id} attempts so far: {t.metadata['retries']}")
                    self._add_proc_task(
                        t.metadata["input"], ds, attempt=t.metadata["retries"] + 1
                    )
                else:
                    raise RuntimeError(
                        f"task could not be completed\n{t.std_output}\n---\n{t.output}"
                    )
        return t

    def submit(self, task):
        task.add_input(self._dynmapred_file, "dynmapred.py")
        task.add_input(self._coffea_dir, "coffea")
        task.set_retries(self.max_task_retries)

        if self._proxy_file:
            task.add_input(self._proxy_file, "proxy.pem")
            task.set_env_var("X509_USER_PROXY", "proxy.pem")

        ds = task.metadata["dataset"]
        self._task_active += 1
        self._ds_active_count[ds] += 1
        tid = self.manager.submit(task)

        return tid

    def generate_tasks(self, datasets):
        for ds, info in datasets.items():
            max = self.max_tasks_per_dataset
            for datum in info:
                yield (datum, ds)
                if max is not None:
                    max -= 1
                    if max < 1:
                        break
            self._ds_all_submitted[ds] = True
        self._all_proc_submitted = True

    def compute(
        self,
        data,
        manager_args=None,
        remote_executor=None,
        remote_executor_args=None,
    ):
        self._set_resources(data)
        data_iter = self.generate_tasks(data)

        while True:
            for datum, dataset in data_iter:
                self._add_proc_task(datum, dataset)
                if self.max_tasks_active and self.max_tasks_active < self._task_active:
                    break

            t = self.wait(5)
            if t:
                self._add_accum_tasks(t)

            if self._all_proc_submitted and self.manager.empty():
                break

        results = {}
        for ds in data:
            f = self._ds_outputs[ds]
            with lz4.frame.open(f"{f.source()}", "rb") as fp:
                results[ds] = cloudpickle.load(fp)

        return results


if __name__ == "__main__":
    data = (da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 1000))
    # data = [da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 10000)]

    def double_data(d):
        return d.map_blocks(lambda x: x * 2)

    def add_data(a, b):
        return a + b

    mgr = vine.Manager(port=9123)
    dmr = DynMapReduce(
        mgr, source_connector=lambda x: x, processor=double_data, accumulator=add_data
    )
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
