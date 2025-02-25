#! /usr/bin/env python

import dask.array as da
import cloudpickle
import lz4.frame
import sys
import math
import re
import os
from pathlib import Path

# import rich
import uuid
import abc
import dataclasses

import ndcctools.taskvine as vine

from typing import Any, Callable, Hashable, Mapping, List, Optional, TypeVar, Self

D = TypeVar("D")
P = TypeVar("P")
H = TypeVar("H")

priority_separation = 1_000_000


def wrap_processing(
    processor,
    source_postprocess,
    datum,
    processor_args,
    source_postprocess_args,
    # local_executor="threads",
    local_executor="processes",
    local_executor_args=None,
):
    import os
    import dask
    from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
    # from distributed import Client, LocalCluster

    if not local_executor_args:
        local_executor_args = {}

    local_executor_args.setdefault("num_workers", 2*int(os.environ.get("CORES", 1)))

    # cluster = LocalCluster(n_workers=local_executor_args["num_workers"])
    # client = Client(cluster)

    if processor_args is None:
        processor_args = {}

    if source_postprocess_args is None:
        source_postprocess_args = {}

    datum_post = source_postprocess(datum, **source_postprocess_args)

    # Configure the appropriate executor based on the scheduler type
    num_workers = local_executor_args["num_workers"]
    scheduler = local_executor_args["scheduler"]

    # Only create a pool if we have workers to use
    pool = None

    if scheduler == "threads":
        pool = ThreadPoolExecutor(max_workers=num_workers)
    elif scheduler == "processes":
        pool = ProcessPoolExecutor(max_workers=num_workers)
    else:
        local_executor_args.setdefault("scheduler", local_executor)

    # Use the configured pool with Dask if available
    if pool:
        with dask.config.set(pool=pool):
            result = processor(datum_post, **processor_args).compute()
        # Make sure to close the pool to free resources
        pool.shutdown()
    else:
        # If no pool is available, just compute without a specific pool
        result = processor(datum_post, **processor_args).compute(**local_executor_args)

    with lz4.frame.open("task_output.p", "wb") as fp:
        cloudpickle.dump(result, fp)


def accumulate(accumulator, result_names, accumulator_args=None, to_file=True):
    out = None

    if accumulator_args is None:
        accumulator_args = {}

    for r in sorted(result_names):
        with lz4.frame.open(r, "rb") as fp:
            other = cloudpickle.load(fp)

        if other is None:
            continue

        if out is None:
            out = other
        else:
            # out = accumulator(out, other, **accumulator_args)
            try:
                out = accumulator(out, other)
            except TypeError:
                pass
            del other

    if to_file:
        with lz4.frame.open("task_output.p", "wb") as fp:
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
        firsts, to_reduce = (
            to_reduce[:accumulator_n_args],
            to_reduce[accumulator_n_args:],
        )
        task_graph[key] = (accumulator_w_kwargs, *firsts)
        to_reduce.append(key)

    out = dask.get(
        task_graph, to_reduce[0], executor=local_executor, **local_executor_args
    )
    if to_file:
        with lz4.frame.open("task_output.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


def identity_source_conector(datum, **extra_args):
    return datum


def identity_source_preprocess(datum, **extra_args):
    yield datum


def default_accumualtor(a, b, **extra_args):
    return a + b


@dataclasses.dataclass
class ProcCounts:
    name: str
    fn: Callable[[P], H]
    priority: int = 0

    def __post_init__(self):
        self._datasets: dict[str, DatasetCounts] = {}
        self.all_proc_submitted = False

    def dataset(self, name):
        return self._datasets.setdefault(name, DatasetCounts(name, self.priority - len(self._datasets)))

    def inc_submitted(self, dataset):
        self.datasets(dataset).inc_submitted()

    def add_completed(self, dataset, task):
        self.datasets(dataset).add_completed(task)

    def set_final(self, dataset, task):
        self.datasets(dataset).set_final(task)


@dataclasses.dataclass
class DatasetCounts:
    name: str
    priority: int

    def __post_init__(self):
        self.all_proc_submitted = False
        self.pending_accumulation = []
        self.done_count = 0
        self.active_count = 0
        self.output_file = None
        self.result = None

    def inc_submitted(self, n=1):
        self.active_count += n

    def add_completed(self, t):
        self.active_count -= 1

        if t.successful():
            if not t.is_checkpoint() and t.manager.should_checkpoint(t):
                t.manager._add_fetch_task(t, final=False)
                return

            self.done_count += 1
            if not t.is_final():
                self.pending_accumulation.append(t)
            if t.is_checkpoint():
                print(
                    f"retrieved {t.description()} {t.checkpoint_distance} {t.exec_time} {t.cumulative_exec_time}"
                )

    def set_final(self, task):
        self.output_file = task.result_file

    def set_result(self, result):
        self.result = result

    def ready_for_result(self):
        return self.all_proc_submitted and self.active_count == 0 and len(self.pending_accumulation) < 2


@dataclasses.dataclass
class DynMapRedTask(abc.ABC):
    manager: vine.Manager
    processor_name: str
    dataset_name: str
    datum: Hashable
    input_tasks: list | None = (
        None  # want list[DynMapRedTask] and list[Self] does not inheret well
    )
    _: dataclasses.KW_ONLY
    checkpoint: bool = False
    final: bool = False
    attempt_number: int = 1
    priority_constant: int = 0

    def __post_init__(self) -> None:
        self._result_file = None
        self._vine_task = None

        self.checkpoint_distance = 1
        if self.input_tasks:
            self.checkpoint_distance += max(
                t.checkpoint_distance for t in self.input_tasks
            )

        self.checkpoint = self.manager.should_checkpoint(self)

        self.processor = self.manager.processors[self.processor_name]
        self.dataset = self.processor.dataset(self.dataset_name)
        self._vine_task = self.create_task(
            self.manager,
            self.processor,
            self.dataset,
            self.datum,
            self.input_tasks,
            self.result_file,
        )

        if self.checkpoint:
            self.checkpoint_distance = 0
            self.priority_constant += 1

        self.set_priority(
            priority_separation ** self.priority_constant + self.dataset.priority
        )

        if self.manager.environment:
            self.vine_task.add_environment(self.manager.environment)

        self.vine_task.set_category(self.description())
        self.vine_task.add_output(self.result_file, "task_output.p")

    def __getattr__(self, attr):
        # redirect any unknown method to inner vine task
        return getattr(self._vine_task, attr, AttributeError)

    def is_checkpoint(self):
        return self.final or self.checkpoint

    def is_final(self):
        return self.final

    @abc.abstractmethod
    def description(self):
        pass

    @property
    def vine_task(self):
        return self._vine_task

    @property
    def result_file(self):
        if not self._result_file:
            if self.is_checkpoint():
                if self.is_final():
                    name = f"{self.manager.results_directory}/{self.processor_name}/{self.dataset_name}"
                else:
                    name = f"{self.manager.staging_directory}/{self.processor_name}/{uuid.uuid4()}"
                self._result_file = self.manager.declare_file(
                    name,
                    cache=(not self.is_final()),
                    unlink_when_done=(not self.is_final()),
                )
            else:
                self._result_file = self.manager.declare_temp()
        return self._result_file

    @property
    def exec_time(self):
        if not self.vine_task or not self.completed():
            return None
        else:
            return self.resources_measured.wall_time

    @property
    def cumulative_exec_time(self):
        if self.is_checkpoint():
            return 0

        cumulative = 0
        if self.input_tasks:
            cumulative += sum(t.cumulative_exec_time for t in self.input_tasks)

        here = self.exec_time
        if here and here > 0:
            cumulative += here

        return cumulative

    @abc.abstractmethod
    def create_task(
        self: Self,
        manager: vine.Manager,
        processor: ProcCounts,
        dataset: DatasetCounts,
        datum: Hashable,
        input_tasks: list | None,
        result_file: vine.File,
    ) -> vine.Task:
        pass

    @abc.abstractmethod
    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        return None

    def cleanup(self):
        # intermediate results can only be cleaned-up from a task with results at the manager
        if not self.is_checkpoint():
            return
        self._cleanup_actual()

    def _cleanup_actual(self):
        while self.input_tasks:
            t = self.input_tasks.pop()
            t._cleanup_actual()
            self.manager.undeclare_file(t.result_file)

    def _clone_next_attempt(self, datum=None, input_tasks=None):
        return type(self)(
            self.manager,
            self.processor_name,
            self.dataset_name,
            datum if datum is not None else self.datum,
            input_tasks if input_tasks is not None else self.input_tasks,
            checkpoint=self.checkpoint,
            final=self.final,
            attempt_number=self.attempt_number + 1,
        )

    def create_new_attempts(self):
        if self.attempt_number >= self.manager.max_task_retries:
            print(self.description())
            print(self.std_output)
            raise RuntimeError(
                f"task {self.id} has reached the maximum number of retries ({self.manager.max_task_retries})"
            )
        new_tasks = []
        if self.result == "resource exhaustion":
            args = self.resubmit_args_on_exhaustion()
            if args:
                for args in self.resubmit_args_on_exhaustion():
                    new_tasks.append(
                        datum=args.get("datum", None),
                        input_tasks=args.get("input_tasks", None),
                    )
        else:
            new_tasks.append(self._clone_next_attempt())

        return new_tasks


class DynMapRedProcessingTask(DynMapRedTask):
    def create_task(
        self: Self,
        manager: vine.Manager,
        processor: ProcCounts,
        dataset: DatasetCounts,
        datum: Hashable,
        input_tasks: list[Self] | None,
        result_file: vine.File,
    ) -> vine.Task:
        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(
            wrap_processing,
            processor.fn,
            manager.source_postprocess,
            datum,
            manager.processor_args,
            manager.source_postprocess_args,
        )

        for k, v in manager.resources_processing.items():
            getattr(task, f"set_{k}")(v)

        return task

    def description(self):
        return f"processing#{self.processor_name}#{self.dataset_name}"

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        return None


class DynMapRedFetchTask(DynMapRedTask):
    def __post_init__(self):
        self.checkpoint = True
        self.priority_constant = 3
        super().__post_init__()

    def create_task(
        self: Self,
        manager: vine.Manager,
        processor: ProcCounts,
        dataset: DatasetCounts,
        datum: Hashable,
        input_tasks,
        result_file: vine.File,
    ) -> vine.Task:

        assert input_tasks is not None and len(input_tasks) == 1

        target = input_tasks[0]

        # task = vine.Task("ln -s task_input.p task_output.p")
        # task = vine.Task("cp task_input.p task_output.p")

        task = vine.Task("ln -L task_input.p task_output.p")
        task.add_input(
            target.result_file, "task_input.p"  # , strict_input=(self.attempt_number == 1)
        )
        task.set_cores(1)

        return task

    def description(self):
        return f"fetching#{self.processor_name}#{self.dataset_name}"

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        # resubmit with the same args
        return [{}]


class DynMapRedAccumTask(DynMapRedTask):
    def __post_init__(self):
        self.checkpoint = True
        self.priority_constant = 2
        super().__post_init__()

    def create_task(
        self: Self,
        manager: vine.Manager,
        processor: ProcCounts,
        dataset: DatasetCounts,
        datum: Hashable,
        input_tasks,
        result_file: vine.File,
    ) -> vine.Task:

        # task = vine.FunctionCall(self._lib_name, "accumulate", self._accumulator, [f"input_{tid}" for tid in firsts])
        task = vine.PythonTask(
            # accumulate_tree,
            accumulate,
            self.manager.accumulator,
            [f"input_{t.id}" for t in input_tasks],
            accumulator_args=self.manager.accumulator_args,
            to_file=True,
        )

        for t in input_tasks:
            task.add_input(t.result_file, f"input_{t.id}")

        task.set_category(f"accumulating#{processor.name}#{dataset.name}")

        for k, v in manager.resources_accumualting.items():
            getattr(task, f"set_{k}")(v)

        return task

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        n = len(self.input_tasks)
        if n < 4 or self.manager.accum_size < 2:
            return None

        if n >= self.manager.accum_size:
            self.manager.accum_size = int(
                math.ceil(self.manager.accum_size / 2)
            )  # this should not be here
            print("reducing accumulation size to {self.accum_size}")

        ts = [
            {"input_tasks": self.input_tasks[0:n]},
            {"input_tasks": self.input_tasks[n:]},
        ]

        # avoid tasks memory leak
        self.input_tasks = []
        return ts

    def description(self):
        return f"accumulating#{self.processor_name}#{self.dataset_name}"


@dataclasses.dataclass
class DynMapReduce:
    manager: vine.Manager
    processors: Callable[[P], H] | List[Callable[[P], H]] | dict[str, [Callable[[P], H]]]
    accumulation_size: int = 10
    accumulator: Callable[[H, H], H] = default_accumualtor
    accumulator_args: Optional[Mapping[str, Any]] = None
    checkpoint_accumulations: bool = False
    checkpoint_fn: Optional[Callable[[DynMapRedTask], bool]] = None
    environment: Optional[str] = None
    extra_files: Optional[list[str]] = None
    file_replication: int = 1
    max_sources_per_dataset: Optional[int] = None
    max_task_retries: int = 5
    max_tasks_active: Optional[int] = None
    max_tasks_submit_batch: Optional[int] = None
    processor_args: Optional[Mapping[str, Any]] = None
    resources_accumualting: Optional[Mapping[str, float]] = None
    resources_processing: Optional[Mapping[str, float]] = None
    results_directory: str = "results"
    result_postprocess: Optional[Callable[[H], Any]] = None
    source_postprocess: Callable[[D], P] = identity_source_conector
    source_postprocess_args: Optional[Mapping[str, Any]] = None
    source_preprocess: Callable[[Any], D] = identity_source_preprocess
    source_preprocess_args: Optional[Mapping[str, Any]] = None
    x509_proxy: Optional[str] = None
    graph_output_file: bool = True

    def __post_init__(self):
        def name(p):
            try:
                n = p.__name__
            except AttributeError:
                n = str(p)
            return re.sub(r'\W', '_', n)

        self._id_to_task = {}
        self._tasks_active = 0

        if isinstance(self.processors, list):
            nps = (len(self.processors) + 1) * priority_separation
            self.processors = {
                name(p): ProcCounts(name(p), p, priority=nps - i * priority_separation)
                for i, p in enumerate(self.processors)
            }
        elif isinstance(self.processors, dict):
            nps = (len(self.processors) + 1) * priority_separation
            self.processors = {
                n: ProcCounts(n, p, priority=nps - i * priority_separation)
                for i, (n, p) in enumerate(self.processors.items())
            }
        else:
            self.processors = {
                name(self.processors): ProcCounts(
                    name(self.processors), self.processors, priority=priority_separation
                )
            }

        if not self.resources_processing:
            self.resources_processing = {"cores": 1}

        if not self.resources_accumualting:
            self.resources_accumualting = {"cores": 1}

        Path(self.results_directory).mkdir(parents=True, exist_ok=True)

        self.manager.tune("hungry-minimum", 100)
        self.manager.tune("prefer-dispatch", 1)
        self.manager.tune("temp-replica-count", self.file_replication)
        self.manager.tune("immediate-recovery", 1)

        self._extra_files_map = {
            "dynmapred.py": self.manager.declare_file(__file__, cache=True)
        }

        if self.x509_proxy:
            self._extra_files_map["proxy.pem"] = self.manager.declare_file(self.x509_proxy, cache=True)

        if self.extra_files:
            for path in self.extra_files:
                self._extra_files_map[os.path.basename(path)] = self.manager.declare_file(path, cache=True)

        self._wait_timeout = 5
        self._graph_file = None
        if self.graph_output_file:
            self._graph_file = open(f"{self.manager.logging_directory}/graph.csv", "w", buffering=1)
            self._graph_file.write("id,category,checkpoint,final,exec_time,cum_time,inputs\n")

        self._set_env()

    def __getattr__(self, attr):
        # redirect any unknown method to inner manager
        return getattr(self.manager, attr)

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

    def _set_resources(self, data):
        for ds in data:
            self.manager.set_category_mode(f"processing#{ds}", "max")
            self.manager.set_category_mode(f"accumulating#{ds}", "max")

            self.manager.set_category_resources_max(
                f"processing#{ds}", self.resources_processing
            )
            self.manager.set_category_resources_max(
                f"accumulating#{ds}", self.resources_accumualting
            )

    def _accum_or_fetch(self, task):
        ds = task.dataset
        if ds.ready_for_result():
            self._add_fetch_task(task, final=True)
        else:
            self._add_accum_task(ds)

    def _set_result(self, target):
        ds = target.dataset
        ds.set_final(target)
        with lz4.frame.open(ds.output_file.source(), "rb") as fp:
            r = cloudpickle.load(fp)
            if self.result_postprocess:
                r = self.result_postprocess(r)
            ds.set_result(r)
            print(f"{target.processor_name}#{target.dataset_name} completed!")

    def _add_fetch_task(self, target, final):
        t = DynMapRedFetchTask(self, target.processor_name, target.dataset_name, None, [target], final=final)
        self.submit(t)

    def _add_accum_task(self, ds):
        final = False
        accum_size = max(2, self.accumulation_size)

        if ds.all_proc_submitted and ds.active_count == 0:
            if len(ds.pending_accumulation) <= accum_size:
                final = True
        elif len(ds.pending_accumulation) < 2 * accum_size - accum_size / 2:
            return

        ds.pending_accumulation.sort(key=lambda t: len(t.input_tasks) if t.input_tasks else 0)

        heads, ds.pending_accumulation = (
            ds.pending_accumulation[:accum_size],
            ds.pending_accumulation[accum_size:],
        )

        first = heads[0]
        t = DynMapRedAccumTask(
            self,
            first.processor_name,
            first.dataset_name,
            None,
            heads,
            checkpoint=self.checkpoint_accumulations,
            final=final,
        )
        self.submit(t)

    @property
    def all_proc_submitted(self):
        return all(p.all_proc_submitted for p in self.processors.values())

    def should_checkpoint(self, t):
        if t.checkpoint or t.final:
            return True
        if self.checkpoint_fn:
            return self.checkpoint_fn(t)
        return False

    def resubmit(self, t):
        print(f"resubmitting task {t.description()} {t.datum}\n{t.std_output}")

        self.manager.undeclare_file(t.result_file)

        new_attempts = t.create_new_attempts()
        if not new_attempts:
            return

        for nt in new_attempts:
            self.submit(nt)

        return True

    def wait(self, timeout):
        tv = self.manager.wait(self._wait_timeout)
        if tv:
            t = self._id_to_task.pop(tv.id)
            self._wait_timeout = 0
            self._tasks_active -= 1

            if t.successful():
                self.write_graph_file(t)
                t.cleanup()
            return t
        else:
            self._wait_timeout = 5
        return None

    def submit(self, task):
        for path, f in self._extra_files_map.items():
            task.add_input(f, path)

        task.set_retries(self.max_task_retries)

        if self.x509_proxy:
            task.set_env_var("X509_USER_PROXY", "proxy.pem")

        self._tasks_active += 1

        task.dataset.inc_submitted()

        tid = self.manager.submit(task.vine_task)
        self._id_to_task[tid] = task

        return tid

    def write_graph_file(self, t):
        if not self._graph_file:
            return

        self._graph_file.write(
            f"{t.id},{t.description()},{t.checkpoint},{t.final},"
            f"{t.exec_time},{t.cumulative_exec_time},"
            f"{':'.join(str(t.id) for t in t.input_tasks or [])}\n"
        )

    def generate_processing_args(self, datasets):
        args = self.source_preprocess_args
        if args is None:
            args = {}

        for p in self.processors.values():
            p.all_proc_submitted = False

            for ds_name, info in datasets.items():
                p.dataset(ds_name).all_proc_submitted = False
                max = self.max_sources_per_dataset
                for datum in info:
                    gen_ds = self.source_preprocess(datum, *args)
                    for sub_datum in gen_ds:
                        yield (p.name, ds_name, sub_datum)
                    if max is not None:
                        max -= 1
                        if max < 1:
                            break
                p.dataset(ds_name).all_proc_submitted = True
            p.all_proc_submitted = True

    def need_to_submit(self):
        max_active = self.max_tasks_active if self.max_tasks_active else sys.maxsize
        max_batch = (
            self.max_tasks_submit_batch if self.max_tasks_submit_batch else sys.maxsize
        )
        hungry = self.manager.hungry()

        # print(f"queue is hungry for {hungry} task(s)")
        return max(0, min(max_active, max_batch, hungry))

    def compute(
        self,
        data,
        manager_args=None,
        remote_executor=None,
        remote_executor_args=None,
    ):
        self._set_resources(data)
        data_iter = self.generate_processing_args(data)

        while True:
            to_submit = self.need_to_submit()
            if to_submit > 0:
                for proc_name, ds_name, datum in data_iter:
                    t = DynMapRedProcessingTask(self, proc_name, ds_name, datum, None)
                    self.submit(t)
                    to_submit -= 1
                    if to_submit < 1:
                        break

            t = self.wait(5)
            if t:
                if t.successful():
                    t.dataset.add_completed(t)
                    if t.is_final():
                        self._set_result(t)
                    else:
                        self._accum_or_fetch(t)

                elif not self.resubmit(t):
                    raise RuntimeError(
                        f"task {t.datum} could not be completed\n{t.std_output}\n---\n{t.output}"
                    )

            if self.all_proc_submitted and self.manager.empty():
                break

        if self._graph_file:
            self._graph_file.flush()
            self._graph_file.close()

        results = {}
        for p in self.processors.values():
            results_proc = {}
            for ds_name in data:
                r = p.dataset(ds_name).result
                results_proc[ds_name] = r
            results[p.name] = results_proc

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
        mgr,
        source_preprocess=preprocess,
        source_postprocess=postprocess,
        processors=double_data,
        accumulator=add_data,
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
