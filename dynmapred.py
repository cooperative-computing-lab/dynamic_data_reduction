#! /usr/bin/env python

import dask.array as da
import cloudpickle
import lz4.frame
import sys
import rich
from dataclasses import dataclass

from collections import defaultdict
import ndcctools.taskvine as vine

from typing import Any, Callable, Dict, Mapping, Optional, TypeVar


def wrap_processing(
    processor,
    source_postprocess,
    datum,
    processor_args=None,
    source_postprocess_args=None,
    local_executor="threads",
    local_executor_args=None,
):
    import os

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    if processor_args is None:
        processor_args = {}

    if source_postprocess_args is None:
        source_postprocess_args = {}

    datum_post = source_postprocess(datum, **source_postprocess_args)

    result = processor(datum_post, **processor_args).compute(
        executor=local_executor, **local_executor_args
    )
    with lz4.frame.open("proc_out.p", "wb") as fp:
        cloudpickle.dump(result, fp)
    print(result)


def accumulate(accumulator, result_names, accumulator_args=None, to_file=True):
    out = None

    if accumulator_args is None:
        accumulator_args = {}

    for r in sorted(result_names):
        with lz4.frame.open(r, "rb") as fp:
            other = cloudpickle.load(fp)

        if out is None:
            out = other
        else:
            #out = accumulator(out, other, **accumulator_args)
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
    accumulator_n_args=2,
    to_file=True,
    from_files=True,
    local_executor="threads",
    local_executor_args=None,
    accumulator_args=None,
):
    import dask
    import os
    from functools import partial

    if not local_executor_args:
        local_executor_args = {}
    local_executor_args.setdefault("nthreads", os.environ.get("CORES", 1))

    if accumulator_args is None:
        accumulator_args = {}

    if from_files:

        def load(filename):
            with lz4.frame.open(filename, "rb") as fp:
                return cloudpickle.load(fp)

    else:

        def load(result):
            return result

    accumulator_w_kwargs = partial(accumulator, **accumulator_args)

    to_reduce = []
    task_graph = {}
    for r in results:
        key = ("load", len(task_graph))
        task_graph[key] = (load, r)
        to_reduce.append(key)

    while len(to_reduce) > 1:
        key = ("merge", len(task_graph))
        firsts, to_reduce = to_reduce[:accumulator_n_args], to_reduce[accumulator_n_args:]
        task_graph[key] = (accumulator_w_kwargs, *firsts)
        to_reduce.append(key)

    out = dask.get(
        task_graph, to_reduce[0], executor=local_executor, **local_executor_args
    )
    if to_file:
        with lz4.frame.open("accum_out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


def identity_source_conector(datum, **extra_args):
    return datum


def identity_source_preprocess(datum, **extra_args):
    yield datum


def default_accumualtor(a, b, **extra_args):
    return a + b


D = TypeVar("D")
P = TypeVar("P")
H = TypeVar("H")


@dataclass
class DynMapReduce:
    manager: vine.Manager
    processor: Callable[[P], H]
    source_preprocess: Callable[[Any], D] = identity_source_preprocess
    source_postprocess: Callable[[D], P] = identity_source_conector
    accumulator: Callable[[H, H], H] = default_accumualtor
    max_tasks_active: Optional[int] = None
    max_tasks_per_dataset: Optional[int] = None
    max_task_retries: int = 2
    accumulation_size: int = 10
    resources_processing: Optional[Mapping[str, float]] = None
    resources_accumualting: Optional[Mapping[str, float]] = None
    processor_args: Optional[Mapping[str, Any]] = None
    source_preprocess_args: Optional[Mapping[str, Any]] = None
    source_postprocess_args: Optional[Mapping[str, Any]] = None
    accumulator_args: Optional[Mapping[str, Any]] = None
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
        task = vine.Task("cp -r in out")

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
        self, datum, dataset, local_executor="threads", local_executor_args=None, attempt=1
    ):
        result_file = self.manager.declare_temp()

        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(
            wrap_processing, self.processor, self.source_postprocess, datum, self.processor_args, self.source_postprocess_args,
        )
        if self.environment:
            task.add_environment(self.environment)

        ds = dataset
        task.set_category(f"processing#{ds}")
        task.metadata = {"type": "processing", "dataset": ds, "attempt": attempt, "input": datum}
        task.add_output(result_file, "proc_out.p")
        self._id_to_output[self.submit(task)] = result_file

    def _add_accum_tasks(self, task, temp_result=True):
        ds = task.metadata["dataset"]

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
            #accumulate_tree,
            accumulate,
            self.accumulator,
            [f"input_{t.id}" for t in firsts],
            accumulator_args=self.accumulator_args,
            to_file=True,
        )

        #result_file = self.manager.declare_temp()
        result_file = self.manager.declare_file("o", cache=True)
        task.add_output(result_file, "accum_out.p")

        for t in firsts:
            task.add_input(self._id_to_output[t.id], f"input_{t.id}")

        task.set_category(f"accumulating#{ds}")
        task.metadata = {"type": "accumulating", "dataset": ds}

        self._id_to_output[self.submit(task)] = result_file

    def wait(self, timeout):
        t = self.manager.wait(5)
        if t:
            print(f"task {t.id} '{t.category}' exit code: {t.exit_code}, vine status: {t.result}")
            print(f"{t.output}")
            print(f"{t.std_output}")

            ds = t.metadata["dataset"]
            self._task_active -= 1
            self._ds_active_count[ds] -= 1
            if t.successful():
                self._ds_done_count[ds] += 1
                self._ds_to_accumulate[ds].append(t)
            else:
                should_retry = t.metadata["type"] == "processing" and t.metadata["attempt"] < self.max_task_retries
                if should_retry:
                    print(f"resubmitting task {t.id} attempts so far: {t.metadata['attempt']}")
                    self._add_proc_task(
                        t.metadata["input"], ds, attempt=t.metadata["attempt"] + 1
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
        args = self.source_preprocess_args
        if args is None:
            args = {}

        for ds, info in datasets.items():
            max = self.max_tasks_per_dataset
            for datum in info:
                for sub_datum in self.source_preprocess(datum, **args):
                    yield (sub_datum, ds)
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
    import itertools
    data = {"some_ds": list(itertools.pairwise(range(1, 13)))}

    # data = [da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 10000)]

    def preprocess(pair):
        yield from pair

    def postprocess(n):
        return da.from_array(list(range(0, n * 1000000, n))[0:10])

    def double_data(datum):
        return datum.map_blocks(lambda x: x * 2)

    def add_data(a, b):
        return a + b

    mgr = vine.Manager(port=[9123, 9129], name="btovar-dynmapred")
    dmr = DynMapReduce(
        mgr, source_preprocess=preprocess, source_postprocess=postprocess, processor=double_data, accumulator=add_data
    )

    workers = vine.Factory("local", manager=mgr)
    workers.max_workers = 1
    workers.min_workers = 0
    workers.cores = 4
    workers.memory = 2000
    workers.disk = 8000
    with workers:
        result = dmr.compute(data)

    print(result)
