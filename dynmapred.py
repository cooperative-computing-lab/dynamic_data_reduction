#! /usr/bin/env python

import dask
import dask.array as da
from functools import reduce
import cloudpickle
import lz4
import sys

import ndcctools.taskvine as vine


def wrap_processing(processor, datum, local_executor="threads", local_executor_args=None):
    if not local_executor_args:
        local_executor_args = {}

    def proc_task():
        result = processor(datum).compute(executor=local_executor, **local_executor_args)
        with lz4.frame.open("out.p", "wb") as fp:
            cloudpickle.dump(result, fp)

    return proc_task


def accumulate(accumulator, result_names, to_file=True):
    out = None

    for r in result_names:
        with lz4.frame.open(r, "rb") as fp:
            other = cloudpickle.load(fp)

        if out is None:
            out = other
        else:
            out = accumulator(out, other)
            del other

    if to_file:
        with lz4.frame.open("out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


class DynMapReduce:
    def __init__(self, manager, source_connector, processor, accumulator):
        self._manager = manager
        self._source_connector = source_connector
        self._processor = processor
        self._accumulator = accumulator

        self._max_active = 5
        self._tasks_active = 0

        self._id_to_output = {}

        self._active_proc_tasks = []
        self._tasks_to_accumulate = []
        self._accumulation_size = 5

        self._last_accum_id = None

    @property
    def manager(self):
        return self._manager

    def _add_proc_task(self, datum, local_executor='threads', local_executor_args=None):
        result_file = self.manager.declare_temp()
        fn = wrap_processing(self._processor, datum)
        task = vine.PythonTask(fn)
        task.set_cores(1)
        task.add_output(result_file, "out.p")
        task.set_category("processing")
        self._id_to_output[self.manager.submit(task)] = result_file

    def _add_accum_tasks(self, temp_result=True):
        if len(self._tasks_to_accumulate) < 1:
            return

        if len(self._tasks_to_accumulate) == 1 and self._last_accum_id:
            return

        set_last = False
        accum_size = self._accumulation_size
        if self.manager.empty():
            set_last = True
            temp_result = False
            accum_size = 1

        while len(self._tasks_to_accumulate) >= accum_size:
            firsts, self._tasks_to_accumulate = (
                self._tasks_to_accumulate[: self._accumulation_size],
                self._tasks_to_accumulate[self._accumulation_size:],
            )

            task = vine.PythonTask(accumulate, self._accumulator, [f"input_{tid}" for tid in firsts])

            if temp_result:
                result_file = self.manager.declare_temp()
            else:
                result_file = self.manager.declare_file(f"out_{id(self)}")

            task.set_cores(1)
            task.add_output(result_file, "out.p")

            for tid in firsts:
                task.add_input(self._id_to_output[tid], f"input_{tid}")

            task.set_category("accumulating")

            tid = self.manager.submit(task)
            self._id_to_output[tid] = result_file

            if set_last:
                self._last_accum_id = tid

    def compute(
        self,
        data,
        manager_args=None,
        remote_executor=None,
        remote_executor_args=None,
    ):
        data = iter(data)

        self._add_proc_task(next(data))

        for d in data:
            self._add_proc_task(d)

        while not self.manager.empty():
            t = self.manager.wait(5)
            if t:
                print(f"task {t.id} {t.category} done: {t.result}")
                if t.successful():
                    last_id = t.id
                    self._tasks_to_accumulate.append(t.id)
                    self._add_accum_tasks()
                else:
                    print(t.std_output)

        if last_id:
            with lz4.frame.open(f"out_{id(self)}", "rb") as fp:
                final_result = cloudpickle.load(fp)

            return final_result


data = [da.from_array(list(range(0, n * 1000000, n))[0:10]) for n in range(1, 10000)]


def double_data(d):
    return d.map_blocks(lambda x: x * 2)


def add_data(a, b):
    return a + b


mgr = vine.Manager(port=9123)


dmr = DynMapReduce(mgr, source_connector=lambda x: x, processor=double_data, accumulator=add_data)


workers = vine.Factory("local", manager=mgr)
workers.max_workers = 4
workers.min_workers = 4
workers.cores = 4
workers.memory = 2000
workers.disk = 8000

with workers:
    result = dmr.compute(data)
    print(result)



sys.exit(0)



c = [double_data(d).compute() for d in data]
print(c)


