#! /usr/bin/env python

import cloudpickle
import lz4.frame
import sys
import math
import re
import os
from pathlib import Path

import uuid
import abc
import dataclasses
import concurrent.futures
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor

import rich.progress
from rich import print

import ndcctools.taskvine as vine


from typing import Any, Callable, Hashable, Mapping, List, Optional, TypeVar, Self

DataT = TypeVar("DataT")
ProcT = TypeVar("ProcT")
ResultT = TypeVar("ResultT")


priority_separation = 1_000_000


def checkpoint_standard(distance, time, size, custom_fn, len_fn, task):
    if distance is not None and task.checkpoint_distance > distance:
        return True

    elif time is not None and task.cumulative_exec_time > time:
        return True

    elif size is not None and len_fn(task) > size:
        return True

    elif custom_fn is not None:
        return custom_fn(task)

    return False


# Define a custom ProcessPoolExecutor that uses cloudpickle
class CloudpickleProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    @staticmethod
    def _cloudpickle_process_worker(serialized_data):
        import cloudpickle

        fn, args, kwargs = cloudpickle.loads(serialized_data)
        return fn(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        self._mp_context = mp.get_context("fork")
        super().__init__(*args, **kwargs, mp_context=self._mp_context)

    def submit(self, fn, *args, **kwargs):
        # Cloudpickle the function and arguments
        fn_dumps = cloudpickle.dumps((fn, args, kwargs))
        # Submit the wrapper with the serialized data
        return super().submit(
            CloudpickleProcessPoolExecutor._cloudpickle_process_worker, fn_dumps
        )


def wrap_processing(
    processor,
    source_postprocess,
    datum,
    processor_args,
    source_postprocess_args,
    remote_executor_args,
):
    import os
    import warnings

    remote_executor_args.setdefault("num_workers", int(os.environ.get("CORES", 1)))
    remote_executor_args.setdefault("scheduler", "threads")

    if processor_args is None:
        processor_args = {}

    if source_postprocess_args is None:
        source_postprocess_args = {}

    datum_post = source_postprocess(datum, **source_postprocess_args)

    # Configure based on the scheduler type
    num_workers = remote_executor_args["num_workers"]
    scheduler = remote_executor_args["scheduler"]

    # Process the data through the processor
    to_maybe_compute = processor(datum_post, **processor_args)

    # Check if the result is a Dask object that needs to be computed
    is_dask_object = hasattr(to_maybe_compute, "computex")
    if is_dask_object:
        # Compute the result based on the scheduler type
        if scheduler == "cloudpickle_processes" and num_workers > 0:
            # Use our custom ProcessPoolExecutor with cloudpickle
            try:
                with CloudpickleProcessPoolExecutor(
                    max_workers=num_workers
                ) as executor:
                    # result = dask.compute(to_maybe_compute,
                    result = to_maybe_compute.compute(
                        scheduler="processes",
                        pool=executor,
                        optimize_graph=True,
                        num_workers=num_workers,
                        max_height=None,
                        max_width=1,
                        subgraphs=False,
                    )
            except Exception as e:
                warnings.warn(
                    f"CloudpickleProcessPoolExecutor failed: {str(e)}. Falling back to default scheduler."
                )
                # result = to_maybe_compute.compute(num_workers=num_workers)
                raise e
        elif scheduler == "threads" or scheduler is None:
            if num_workers < 2:
                result = to_maybe_compute.compute(scheduler="threads", num_workers=1)
            else:
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    result = to_maybe_compute.compute(
                        scheduler="threads", pool=executor, num_workers=num_workers
                    )
    else:
        # If not a Dask object, just use the result directly and try to materialize it from virtual arrays
        result = to_maybe_compute

        # Add some debugging information
        try:
            result = to_maybe_compute.compute()
        except Exception as e:
            warnings.warn(f"Materialization failed: {str(e)}")

    with lz4.frame.open("task_output.p", "wb") as fp:
        cloudpickle.dump(result, fp)

    try:
        return len(result)
    except Exception:
        return 1


def accumulate(
    accumulator,
    result_names,
    *,
    write_fn,
    results_dir,
    processor_name,
    dataset_name,
    force,
):
    out = None
    for r in sorted(result_names):
        with lz4.frame.open(r, "rb") as fp:
            other = cloudpickle.load(fp)

        if other is None:
            continue

        if out is None:
            out = other
            continue

        try:
            out = accumulator(out, other)
        except TypeError:
            print(f"TYPE_ERROR: {r}")
            raise
        del other

    try:
        size = len(out)
    except Exception:
        size = len(result_names)

    keep_accumulating = True
    if write_fn:
        keep_accumulating = write_fn(
            out, results_dir, processor_name, dataset_name, size, force
        )

    with lz4.frame.open("task_output.p", "wb") as fp:
        if not keep_accumulating:
            out = None
            size = 0
        cloudpickle.dump(out, fp)
        return size


def accumulate_tree(
    accumulator,
    results,
    accumulator_n_args=2,
    from_files=True,
    local_executor_args=None,
):
    import dask
    import os

    if not local_executor_args:
        local_executor_args = {}

    local_executor_args.setdefault("scheduler", "threads")
    local_executor_args.setdefault("num_workers", os.environ.get("CORES", 1))

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
        firsts, to_reduce = (
            to_reduce[:accumulator_n_args],
            to_reduce[accumulator_n_args:],
        )
        task_graph[key] = (accumulator, *firsts)
        to_reduce.append(key)

    out = dask.get(task_graph, to_reduce[0], **local_executor_args)

    with lz4.frame.open("task_output.p", "wb") as fp:
        cloudpickle.dump(out, fp)

    try:
        return len(out)
    except Exception:
        return len(results)


def identity_source_conector(datum, **extra_args):
    return datum


def identity_source_preprocess(dataset_info, **extra_args):
    for datum in dataset_info:
        yield (datum, 1)


def default_accumualtor(a, b, **extra_args):
    return a + b


@dataclasses.dataclass
class ProcCounts:
    workflow: object  # really a DynamicDataReduction, but typing in python is a pain
    name: str
    fn: Callable[[ProcT], ResultT]
    priority: int = 0

    def __post_init__(self):
        self._datasets = {}

        for ds_name, ds_specs in self.workflow.data["datasets"].items():
            self.add_dataset(ds_name, ds_specs)

    @property
    def all_proc_done(self):
        return all(ds.all_proc_done for ds in reversed(self._datasets.values()))

    @property
    def items_done(self):
        return sum(ds.items_done for ds in self._datasets.values())

    @property
    def items_failed(self):
        return sum(ds.items_failed for ds in self._datasets.values())

    @property
    def items_submitted(self):
        return sum(ds.items_submitted for ds in self._datasets.values())

    @property
    def items_total(self):
        return sum(ds.items_total for ds in self._datasets.values())

    @property
    def proc_tasks_done(self):
        return sum(ds.proc_tasks_done for ds in self._datasets.values())

    @property
    def proc_tasks_failed(self):
        return sum(ds.proc_tasks_failed for ds in self._datasets.values())

    @property
    def proc_tasks_submitted(self):
        return sum(ds.proc_tasks_submitted for ds in self._datasets.values())

    @property
    def proc_tasks_total(self):
        items_total = self.items_total
        items_submitted = self.items_submitted
        tasks_submitted_good = self.proc_tasks_submitted - self.proc_tasks_failed

        if items_total == 0:
            return 0
        elif items_submitted == 0:
            return 1
        elif tasks_submitted_good == 0:
            return math.ceil(
                (items_total / items_submitted) * self.proc_tasks_submitted
            )
        else:
            return math.ceil((items_total / items_submitted) * tasks_submitted_good)

    @property
    def accum_tasks_done(self):
        return sum(ds.accum_tasks_done for ds in self._datasets.values())

    @property
    def accum_tasks_submitted(self):
        return sum(ds.accum_tasks_submitted for ds in self._datasets.values())

    @property
    def accum_tasks_total(self):
        left = (
            self.proc_tasks_total
            - self.proc_tasks_done
            + self.accum_tasks_submitted
            - self.accum_tasks_done
        )

        if left <= 0:
            return self.accum_tasks_submitted

        total_accums = self.accum_tasks_submitted
        while left > self.workflow.accumulation_size:
            accs, left = divmod(left, self.workflow.accumulation_size)
            total_accums += accs
            left += accs
        if left > 0:
            total_accums += 1

        return total_accums

    def __hash__(self):
        return id(self)

    def add_dataset(self, dataset_name, dataset_specs):
        args = self.workflow.source_preprocess_args
        if args is None:
            args = {}

        gen = self.workflow.source_preprocess(dataset_specs, **args)
        size = 0
        for _, pre_size in gen:
            size += pre_size

        self._datasets[dataset_name] = DatasetCounts(
            self,
            dataset_name,
            self.priority - len(self._datasets),
            size,
        )

    def dataset(self, name):
        return self._datasets[name]

    def add_active(self, task):
        self.dataset(task.dataset.name).add_active(task)

        self.workflow.progress_bars.update(
            self,
            "procs",
            total=self.proc_tasks_total,
            completed=self.proc_tasks_done,
            description=f"procs ({self.name}): {self.proc_tasks_done} done, {self.proc_tasks_failed} failed",
        )
        self.workflow.progress_bars.update(
            self,
            "accums",
            total=self.accum_tasks_total,
        )

    def add_completed(self, task):
        self.dataset(task.dataset.name).add_completed(task)
        if not task.successful():
            return

        self.workflow.progress_bars.update(
            self,
            "accums",
            completed=self.accum_tasks_done,
        )
        if isinstance(task, DynMapRedProcessingTask):
            self.workflow.progress_bars.update(
                self,
                "procs",
                completed=self.proc_tasks_done,
                description=f"procs ({self.name}): {self.proc_tasks_done} done, {self.proc_tasks_failed} failed",
            )
            # Update items progress bar to include both successful and failed items
            self.workflow.progress_bars.update(
                self,
                "items",
                completed=self.items_done + self.items_failed,
                description=f"items ({self.name}): {self.items_done} done, {self.items_failed} failed",
            )

    def refresh_progress_bars(self):
        self.workflow.progress_bars.update(
            self,
            "items",
            total=self.items_total,
            completed=self.items_done + self.items_failed,
            description=f"items ({self.name}): {self.items_done} done, {self.items_failed} failed",
        )
        self.workflow.progress_bars.update(
            self,
            "procs",
            total=self.proc_tasks_total,
            completed=self.proc_tasks_done,
            description=f"procs ({self.name}): {self.proc_tasks_done} done, {self.proc_tasks_failed} failed",
        )
        self.workflow.progress_bars.update(
            self,
            "accums",
            total=self.accum_tasks_total,
        )
        self.workflow.progress_bars.refresh()


@dataclasses.dataclass
class DatasetCounts:
    processor: ProcCounts
    name: str
    priority: int
    items_total: int

    def __post_init__(self):
        self.pending_accumulation = []
        self.output_file = None
        self.result = None
        self.active = set()

        self.items_done = 0
        self.items_failed = 0
        self.items_submitted = 0
        self.proc_tasks_done = 0
        self.proc_tasks_failed = 0
        self.proc_tasks_submitted = 0
        self.accum_tasks_done = 0
        self.accum_tasks_submitted = 0
        self.accum_tasks_checkpointed = 0

    @property
    def all_proc_done(self):
        return (self.items_done + self.items_failed) == self.items_total

    def add_completed(self, task):
        self.active.remove(task.id)

        if not task.successful():
            if isinstance(task, DynMapRedProcessingTask):
                self.proc_tasks_failed += 1
            return

        if task.is_checkpoint():
            self.accum_tasks_checkpointed += 1

        if isinstance(task, DynMapRedProcessingTask):
            self.proc_tasks_done += 1
            self.items_done += task.input_size
        elif isinstance(task, DynMapRedAccumTask):
            self.accum_tasks_done += 1

    def add_active(self, task):
        if isinstance(task, DynMapRedProcessingTask):
            self.proc_tasks_submitted += 1
        elif isinstance(task, DynMapRedAccumTask):
            self.accum_tasks_submitted += 1

        self.active.add(task.id)

    def set_result(self, task):
        print(f"{self.processor.name}#{self.name} completed!")
        r = None
        if task:
            self.output_file = task.result_file
            with lz4.frame.open(self.output_file.source(), "rb") as fp:
                r = cloudpickle.load(fp)
            if self.processor.workflow.result_postprocess:
                dir = self.processor.workflow.results_directory
                r = self.processor.workflow.result_postprocess(
                    self.processor.name, self.name, dir, r
                )
        self.result = r
        self.processor.workflow.progress_bars.advance(self.processor, "datasets", 1)
        for bar_type in [
            "datasets",
            "items",
            "procs",
            "accums",
        ]:
            self.processor.workflow.progress_bars.stop_task(self.processor, bar_type)

    def ready_for_result(self):
        return (
            self.all_proc_done
            and len(self.active) == 0
            and len(self.pending_accumulation) == 0
            and self.items_total == (self.items_done + self.items_failed)
        )


@dataclasses.dataclass
class DynMapRedTask(abc.ABC):
    manager: vine.Manager
    processor: ProcCounts
    dataset: DatasetCounts
    datum: Hashable
    _: dataclasses.KW_ONLY
    input_tasks: list | None = (
        None  # want list[DynMapRedTask] and list[Self] does not inheret well
    )
    checkpoint: bool = False
    final: bool = False
    attempt_number: int = 1
    priority_constant: int = 0
    input_size: int = 1
    output_size: Optional[int] = None

    def __post_init__(self) -> None:
        self._result_file = None
        self._vine_task = None

        self.checkpoint_distance = 1
        if self.input_tasks:
            self.checkpoint_distance += max(
                t.checkpoint_distance for t in self.input_tasks
            )

        self._cumulative_inputs_time = 0
        if self.input_tasks:
            self._cumulative_inputs_time = sum(
                t.exec_time + t.cumulative_inputs_time
                for t in self.input_tasks
                if not t.is_checkpoint()
            )

        self.checkpoint = self.manager.should_checkpoint(self)

        self._vine_task = self.create_task(
            self.datum,
            self.input_tasks,
            self.result_file,
        )

        if self.checkpoint:
            self.checkpoint_distance = 0
            self.priority_constant += 1

        self.set_priority(
            priority_separation**self.priority_constant + self.dataset.priority
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
                    name = f"{self.manager.results_directory}/raw/{self.processor.name}/{self.dataset.name}"
                else:
                    name = f"{self.manager.staging_directory}/{self.processor.name}/{uuid.uuid4()}"
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
    def cumulative_inputs_time(self):
        return self._cumulative_inputs_time

    @property
    def cumulative_exec_time(self):
        if self.is_checkpoint():
            return 0

        cumulative = 0
        if self.input_tasks:
            cumulative = sum(t.cumulative_exec_time for t in self.input_tasks)

        here = self.exec_time
        if here and here > 0:
            cumulative += here

        return cumulative

    @abc.abstractmethod
    def create_task(
        self: Self,
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
        if not self.is_checkpoint() and self.output_size > 0:
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
            self.processor,
            self.dataset,
            datum if datum is not None else self.datum,
            input_tasks=input_tasks if input_tasks is not None else self.input_tasks,
            checkpoint=self.checkpoint,
            final=self.final,
            attempt_number=self.attempt_number + 1,
            input_size=self.input_size,
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
                        self._clone_next_attempt(
                            datum=args.get("datum", None),
                            input_tasks=args.get("input_tasks", None),
                        )
                    )
        else:
            new_tasks.append(self._clone_next_attempt())

        return new_tasks


class DynMapRedProcessingTask(DynMapRedTask):
    def create_task(
        self: Self,
        datum: Hashable,
        input_tasks: list[Self] | None,
        result_file: vine.File,
    ) -> vine.Task:
        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(
            wrap_processing,
            self.processor.fn,
            self.manager.source_postprocess,
            datum,
            self.manager.processor_args,
            self.manager.source_postprocess_args,
            self.manager.remote_executor_args,
        )

        for k, v in self.manager.resources_processing.items():
            # Handle wall_time specially - it uses set_time_max() instead of set_wall_time()
            if k == "wall_time":
                task.set_time_max(v)
            else:
                getattr(task, f"set_{k}")(v)

        return task

    def description(self):
        return f"processing#{self.processor.name}#{self.dataset.name}"

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        return None


class DynMapRedFetchTask(DynMapRedTask):
    def __post_init__(self):
        self.checkpoint = True
        self.priority_constant = 2
        super().__post_init__()

    def create_task(
        self: Self,
        datum: Hashable,
        input_tasks,
        result_file: vine.File,
    ) -> vine.Task:

        assert input_tasks is not None and len(input_tasks) == 1
        target = input_tasks[0]

        task = vine.Task("ln -L task_input.p task_output.p")
        task.add_input(
            target.result_file,
            "task_input.p",  # , strict_input=(self.attempt_number == 1)
        )
        task.set_cores(1)

        return task

    def description(self):
        return f"fetching#{self.processor.name}#{self.dataset.name}"

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        # resubmit with the same args
        return [{}]


class DynMapRedAccumTask(DynMapRedTask):
    def __post_init__(self):
        self.priority_constant = 2
        self.input_size = sum(t.output_size for t in self.input_tasks)
        super().__post_init__()

    def create_task(
        self: Self,
        datum: Hashable,
        input_tasks,
        result_file: vine.File,
    ) -> vine.Task:

        task = vine.PythonTask(
            accumulate,
            self.manager.accumulator,
            [f"input_{t.id}" for t in input_tasks],
            write_fn=self.manager.checkpoint_postprocess,
            results_dir=self.manager.results_directory,
            processor_name=self.processor.name,
            dataset_name=self.dataset.name,
            force=self.final,
        )

        for t in input_tasks:
            task.add_input(t.result_file, f"input_{t.id}")

        task.set_category(f"accumulating#{self.processor.name}#{self.dataset.name}")

        for k, v in self.manager.resources_accumualting.items():
            # Handle wall_time specially - it uses set_time_max() instead of set_wall_time()
            if k == "wall_time":
                task.set_time_max(v)
            else:
                getattr(task, f"set_{k}")(v)

        return task

    def resubmit_args_on_exhaustion(self: Self) -> list[dict[Any, Any]] | None:
        n = len(self.input_tasks)
        if n < 4 or self.manager.accumulation_size < 2:
            return None

        if n >= self.manager.accumulation_size:
            self.manager.accumulation_size = int(
                math.ceil(self.manager.accumulation_size / 2)
            )  # this should not be here
            print(f"reducing accumulation size to {self.manager.accumulation_size}")

        ts = [
            {"input_tasks": self.input_tasks[0:n]},
            {"input_tasks": self.input_tasks[n:]},
        ]

        # avoid tasks memory leak
        self.input_tasks = []
        return ts

    def description(self):
        return f"accumulating#{self.processor.name}#{self.dataset.name}"


@dataclasses.dataclass
class DynamicDataReduction:
    manager: vine.Manager
    processors: (
        Callable[[ProcT], ResultT]
        | List[Callable[[ProcT], ResultT]]
        | dict[str, Callable[[ProcT], ResultT]]
    )
    data: dict[str, dict[str, Any]]
    result_length: Callable[[ResultT], int] = len
    accumulation_size: int = 10
    accumulator: Optional[Callable[[ResultT, ResultT], ResultT]] = default_accumualtor
    checkpoint_accumulations: bool = False
    checkpoint_size: Optional[int] = None
    checkpoint_distance: Optional[int] = None
    checkpoint_time: Optional[int] = None
    checkpoint_custom_fn: Optional[Callable[[DynMapRedTask], bool]] = None
    environment: Optional[str] = None
    extra_files: Optional[list[str]] = None
    file_replication: int = 1
    max_task_retries: int = 5
    max_tasks_active: Optional[int] = None
    max_tasks_submit_batch: Optional[int] = None
    processor_args: Optional[Mapping[str, Any]] = None
    remote_executor_args: Optional[Mapping[str, Any]] = None
    resources_accumualting: Optional[Mapping[str, float]] = None
    resources_processing: Optional[Mapping[str, float]] = None
    results_directory: str = "results"
    result_postprocess: Optional[Callable[[str, str, str, ResultT], Any]] = None
    checkpoint_postprocess: Optional[Callable[[ResultT, str, str, str, bool], int]] = (
        None
    )
    source_postprocess: Callable[[DataT], ProcT] = identity_source_conector
    source_postprocess_args: Optional[Mapping[str, Any]] = None
    source_preprocess: Callable[[Any], DataT] = identity_source_preprocess
    source_preprocess_args: Optional[Mapping[str, Any]] = None
    x509_proxy: Optional[str] = None
    graph_output_file: bool = True
    skip_datasets: Optional[List[str]] = None
    resource_monitor: str | bool | None = "measure"
    verbose: bool = True

    def __post_init__(self):
        def name(p):
            try:
                n = p.__name__
            except AttributeError:
                n = str(p)
            return re.sub(r"\W", "_", n)

        self._id_to_task = {}
        self.datasets_failed = set()

        if isinstance(self.processors, list):
            nps = (len(self.processors) + 1) * priority_separation
            self.processors = {
                name(p): ProcCounts(
                    self, name(p), p, priority=nps - i * priority_separation
                )
                for i, p in enumerate(self.processors)
            }
        elif isinstance(self.processors, dict):
            nps = (len(self.processors) + 1) * priority_separation
            self.processors = {
                n: ProcCounts(self, n, p, priority=nps - i * priority_separation)
                for i, (n, p) in enumerate(self.processors.items())
            }
        else:
            self.processors = {
                name(self.processors): ProcCounts(
                    self,
                    name(self.processors),
                    self.processors,
                    priority=priority_separation,
                )
            }

        if self.accumulator is None:
            self.accumulator = default_accumualtor

        if not self.resources_processing:
            self.resources_processing = {"cores": 1}

        if not self.resources_accumualting:
            self.resources_accumualting = {"cores": 1}

        if not self.remote_executor_args:
            self.remote_executor_args = {}

        results_dir = Path(self.results_directory).absolute()
        results_dir.mkdir(parents=True, exist_ok=True)

        self.manager.tune("hungry-minimum", 100)
        self.manager.tune("prefer-dispatch", 1)
        self.manager.tune("temp-replica-count", self.file_replication)
        self.manager.tune("immediate-recovery", 1)

        # Configure resource monitoring
        self._configure_resource_monitoring()

        self._extra_files_map = {
            "dynmapred.py": self.manager.declare_file(__file__, cache=True)
        }

        if self.x509_proxy:
            self._extra_files_map["proxy.pem"] = self.manager.declare_file(
                self.x509_proxy, cache=True
            )

        if self.extra_files:
            for path in self.extra_files:
                self._extra_files_map[os.path.basename(path)] = (
                    self.manager.declare_file(path, cache=True)
                )

        self._wait_timeout = 5
        self._graph_file = None
        if self.graph_output_file:
            self._graph_file = open(
                f"{self.manager.logging_directory}/graph.csv", "w", buffering=1
            )
            self._graph_file.write(
                "id,category,checkpoint,final,exec_time,cum_time,inputs\n"
            )

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

    def _configure_resource_monitoring(self):
        """Configure taskvine resource monitoring based on the resource_monitor parameter."""
        # Handle backward compatibility for boolean values
        if isinstance(self.resource_monitor, bool):
            if self.resource_monitor:
                monitor_mode = "measure"
            else:
                monitor_mode = "off"
        elif self.resource_monitor is None:
            monitor_mode = "off"
        else:
            monitor_mode = self.resource_monitor

        # Configure monitoring based on the mode
        if monitor_mode == "off":
            # No monitoring - do nothing
            pass
        elif monitor_mode == "measure":
            # Basic resource measurement without watchdog
            self.manager.enable_monitoring(watchdog=False, time_series=False)
        elif monitor_mode == "watchdog":
            # Resource measurement with watchdog
            self.manager.enable_monitoring(watchdog=True, time_series=False)
        else:
            raise ValueError(
                f"Invalid resource_monitor value: {self.resource_monitor}. "
                f"Must be one of: 'measure', 'watchdog', 'off', True, False, or None"
            )

    def _print_task_resources(self, task):
        """Print resource information for a completed task if verbose is enabled."""
        if not self.verbose or not task.completed():
            return

        try:
            # Get resource information
            requested = task.resources_allocated
            measured = task.resources_measured
            wall_time = task.get_metric("time_workers_execute_last") / 1e6 # convert microseconds to seconds

            print(f"Task {task.description()} resources:")
            print(
                f"  Allocated: cores={requested.cores}, memory={requested.memory} MB, disk={requested.disk} MB"
            )
            print(
                f"  Measured:  cores={measured.cores}, memory={measured.memory} MB, disk={measured.disk} MB, wall_time={wall_time:.3f} s"
            )
        except Exception as e:
            # If resource monitoring is not enabled, resources_measured might not be available
            print(
                f"Task {task.description()} resources: (monitoring not available - {e})"
            )

    def _set_resources(self):
        for ds in self.data["datasets"]:
            self.manager.set_category_mode(f"processing#{ds}", "max")
            self.manager.set_category_mode(f"accumulating#{ds}", "max")

            self.manager.set_category_resources_max(
                f"processing#{ds}", self.resources_processing
            )
            self.manager.set_category_resources_max(
                f"accumulating#{ds}", self.resources_accumualting
            )

    def add_fetch_task(self, target, final):
        t = DynMapRedFetchTask(
            self,
            target.processor,
            target.dataset,
            None,
            input_tasks=[target],
            final=final,
        )
        self.submit(t)

    def add_accum_task(self, dataset, task):
        ds = dataset
        if task and task.output_size > 0:
            ds.pending_accumulation.append(task)

        final = False
        accum_size = max(2, self.accumulation_size)
        if ds.all_proc_done and len(ds.active) == 0:
            if len(ds.pending_accumulation) <= accum_size:
                final = True
        elif len(ds.pending_accumulation) < 2 * accum_size:
            return

        if final and len(ds.pending_accumulation) == 0:
            ds.set_result(None)
            return

        ds.pending_accumulation.sort(
            key=lambda t: t.output_size if t.output_size else len(t.input_tasks)
        )

        heads, ds.pending_accumulation = (
            ds.pending_accumulation[:accum_size],
            ds.pending_accumulation[accum_size:],
        )

        first = heads[0]
        t = DynMapRedAccumTask(
            self,
            first.processor,
            first.dataset,
            None,
            input_tasks=heads,
            checkpoint=self.checkpoint_accumulations,
            final=final,
        )
        self.submit(t)

    @property
    def all_proc_done(self):
        return all(p.all_proc_done for p in reversed(self.processors.values()))

    def should_checkpoint(self, task):
        if task.checkpoint or task.final:
            return True
        return checkpoint_standard(
            self.checkpoint_distance,
            self.checkpoint_time,
            self.checkpoint_size,
            self.checkpoint_custom_fn,
            self.result_length,
            task,
        )

    def resubmit(self, task):
        print(f"resubmitting task {task.description()} {task.datum}\n{task.std_output}")

        self.manager.undeclare_file(task.result_file)

        new_attempts = task.create_new_attempts()
        if not new_attempts:
            return False

        for nt in new_attempts:
            self.submit(nt)

        return True

    def wait(self, timeout):
        tv = self.manager.wait(self._wait_timeout)
        if tv:
            t = self._id_to_task.pop(tv.id)
            self._wait_timeout = 0

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

        tid = self.manager.submit(task.vine_task)
        self._id_to_task[tid] = task
        task.processor.add_active(task)

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

        # for p in reversed(self.processors.values()):
        for p in self.processors.values():
            for ds_name, ds_specs in datasets.items():
                ds = p.dataset(ds_name)
                gen = self.source_preprocess(ds_specs, **args)
                for datum, pre_size in gen:
                    ds.items_submitted += pre_size
                    yield (p, ds, datum, pre_size)

    def need_to_submit(self):
        max_active = self.max_tasks_active if self.max_tasks_active else sys.maxsize
        max_batch = (
            self.max_tasks_submit_batch if self.max_tasks_submit_batch else sys.maxsize
        )
        hungry = self.manager.hungry()

        return max(0, min(max_active, max_batch, hungry))

    def add_completed(self, task):
        p = task.processor
        ds = task.dataset

        p.add_completed(task)

        # Print resource information if verbose is enabled
        self._print_task_resources(task)

        if task.successful():
            task.output_size = task.output
            if task.is_checkpoint():
                print(
                    f"chkpt {task.description()} {task.cumulative_inputs_time + task.exec_time:.2f}(s), size: {task.output_size}/{task.input_size})"
                )

            self.write_graph_file(task)

            if task.is_final():
                ds.set_result(task)
            elif ds.ready_for_result():
                self.add_fetch_task(task, final=True)
            elif not task.is_checkpoint() and self.should_checkpoint(task):
                self.add_fetch_task(task, final=False)
            else:
                self.add_accum_task(task.dataset, task)
            task.cleanup()
        else:
            try:
                resubmitted = False
                resubmitted = self.resubmit(task)
            except Exception as e:
                print(e)
            finally:
                if not resubmitted:
                    self.datasets_failed.add(task.dataset.name)
                    # Track failed items when a processing task cannot be resubmitted
                    if isinstance(task, DynMapRedProcessingTask):
                        task.dataset.items_failed += task.input_size
                    self.add_accum_task(task.dataset, None)
                    print(
                        f"task {task.datum} could not be completed\n{task.std_output}\n---\n{task.output}"
                    )
                    task.cleanup()

    def refresh_progress_bars(self):
        for p in self.processors.values():
            p.refresh_progress_bars()

    def compute(self):
        self.progress_bars = ProgressBar()
        for p in self.processors.values():
            self.progress_bars.add_task(p, "datasets", total=len(self.data["datasets"]))
            self.progress_bars.add_task(p, "items", total=p.items_total)
            self.progress_bars.add_task(p, "procs", total=p.proc_tasks_total)
            self.progress_bars.add_task(p, "accums", total=p.accum_tasks_total)

        result = self._compute_internal()
        self.refresh_progress_bars()

        # Print failed items summary
        failed_summary = {}
        for p in self.processors.values():
            for ds_name in self.data["datasets"]:
                ds = p.dataset(ds_name)
                if ds.items_failed > 0:
                    if p.name not in failed_summary:
                        failed_summary[p.name] = {}
                    failed_summary[p.name][ds_name] = ds.items_failed

        if failed_summary:
            print("\nFAILED ITEMS SUMMARY:")
            print("=" * 50)
            for proc_name, datasets in failed_summary.items():
                print(f"Processor: {proc_name}")
                for ds_name, failed_count in datasets.items():
                    print(f"  Dataset '{ds_name}': {failed_count} items failed")
                print()
            print("=" * 50)

        if self.datasets_failed:
            print("WARNING: The following datasets were not completely processed:")
            print(
                "--------------------------------------------------------------------------------"
            )
            for name in self.datasets_failed:
                print(name)
            print(
                "--------------------------------------------------------------------------------"
            )

        return result

    def _compute_internal(self):
        self._set_resources()
        item_generator = self.generate_processing_args(self.data["datasets"])

        while True:
            to_submit = self.need_to_submit()
            if to_submit > 0:
                for proc_name, ds_name, datum, size in item_generator:
                    task = DynMapRedProcessingTask(
                        self,
                        proc_name,
                        ds_name,
                        datum,
                        input_tasks=None,
                        input_size=size,
                    )
                    self.submit(task)
                    to_submit -= 1
                    if to_submit < 1:
                        break

            task = self.wait(5)
            if task:
                self.add_completed(task)

            if self.all_proc_done and self.manager.empty():
                break

        if self._graph_file:
            self._graph_file.flush()
            self._graph_file.close()

        results = {}
        for p in self.processors.values():
            results_proc = {}
            for ds_name in self.data["datasets"]:
                r = p.dataset(ds_name).result
                results_proc[ds_name] = r
            results[p.name] = results_proc

        return results


class ProgressBar:
    @staticmethod
    def make_progress_bar():
        return rich.progress.Progress(
            rich.progress.TextColumn("[bold blue]{task.description}", justify="left"),
            rich.progress.BarColumn(bar_width=None),
            rich.progress.MofNCompleteColumn(),
            "[",
            rich.progress.TimeElapsedColumn(),
            "<",
            rich.progress.TimeRemainingColumn(),
            "]",
            transient=False,
            auto_refresh=True,
        )

    def __init__(self, enabled=True):
        self._prog = self.make_progress_bar()
        self._ids = {}
        if enabled:
            self._prog.start()

    def bar_name(self, p, bar_type):
        return f"{bar_type} ({p.name})"

    def add_task(self, p, bar_type, *args, **kwargs):
        b = self._prog.add_task(self.bar_name(p, bar_type), *args, **kwargs)
        self._ids.setdefault(p, {})[bar_type] = b
        self._prog.start_task(self._ids[p][bar_type])
        return b

    def stop_task(self, p, bar_type, *args, **kwargs):
        return self._prog.stop_task(self._ids[p][bar_type], *args, **kwargs)

    def update(self, p, bar_type, *args, **kwargs):
        return self._prog.update(self._ids[p][bar_type], *args, **kwargs)

    def advance(self, p, bar_type, *args, **kwargs):
        result = self._prog.advance(self._ids[p][bar_type], *args, **kwargs)
        return result

    def refresh(self, *args, **kwargs):
        return self._prog.refresh(*args, **kwargs)

    # redirect anything else to rich_bar
    def __getattr__(self, name):
        return getattr(self._prog, name)
