#! /usr/bin/env python

import dask
import dask.array as da

from functools import reduce
import cloudpickle
import lz4.frame
import sys

import ndcctools.taskvine as vine


def wrap_processing(processor, source_connector, datum, local_executor="threads", local_executor_args=None):
    if not local_executor_args:
        local_executor_args = {}

    datum_post = source_connector(datum)

    result = processor(datum_post).compute(executor=local_executor, **local_executor_args)
    with lz4.frame.open("proc_out.p", "wb") as fp:
        cloudpickle.dump(result, fp)
    return result


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
        with lz4.frame.open("accum_out.p", "wb") as fp:
            cloudpickle.dump(out, fp)
    else:
        return out


def accumulate_tree(accumulator, result_names, to_file=True, local_executor='threads', local_executor_args=None):
    import dask
    if not local_executor_args:
        local_executor_args = {}

    def load_file(filename):
        with lz4.frame.open(r, "rb") as fp:
            return cloudpickle.load(fp)

    to_reduce = []
    task_graph = {}
    for r in result_names:
        key = ("load", len(task_graph))
        task_graph[key] = (load_file, r)
        to_reduce.append(key)

    while len(to_reduce) > 1:
        key = ("merge", len(task_graph))
        firsts, to_reduce = to_reduce[0:2], to_reduce[2:]
        task_graph[key] = (accumulator, *firsts)
        to_reduce.append(key)

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

        self._active_proc_tasks = []
        self._tasks_to_accumulate = []
        self._accumulation_size = 5

        self._last_accum_id = None

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

    def _add_proc_task(self, datum, local_executor='threads', local_executor_args=None):
        result_file = self.manager.declare_temp()
        #task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)

        # task = vine.FunctionCall(self._lib_name, 'wrap_processing', self._processor, datum)
        task = vine.PythonTask(wrap_processing, self._processor, self._source_connector, datum)
        # task.add_environment(self._env)

        task.set_cores(1)
        task.add_output(result_file, "proc_out.p")
        task.set_category("processing")
        self._id_to_output[self.submit(task)] = result_file

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

            # task = vine.FunctionCall(self._lib_name, "accumulate", self._accumulator, [f"input_{tid}" for tid in firsts])
            task = vine.PythonTask(
                accumulate_tree,
                self._accumulator,
                [f"input_{tid}" for tid in firsts],
                local_executor_args={"cores": 4},
            )
            # task.add_environment(self._env)

            if temp_result:
                result_file = self.manager.declare_temp()
            else:
                result_file = self.manager.declare_file(f"out_{id(self)}")

            task.set_cores(1)
            task.add_output(result_file, "accum_out.p")

            for tid in firsts:
                task.add_input(self._id_to_output[tid], f"input_{tid}")

            task.set_category("accumulating")

            tid = self.submit(task)
            self._id_to_output[tid] = result_file

            if set_last:
                self._last_accum_id = tid

    def wait(self, timeout):
        t = self.manager.wait(5)
        if t:
            self._task_active.remove(t.id)
            print(f"task {t.id} {t.category} done: {t.result}")
        return t

    def submit(self, task):
        task.add_input(self._dynmapred_file, "dynmapred.py")
        task.add_input(self._coffea_dir, "coffea")
        tid = self.manager.submit(task)
        self._task_active.add(tid)
        return tid

    def compute(
        self,
        data,
        manager_args=None,
        remote_executor=None,
        remote_executor_args=None,
    ):
        data = iter(data)

        self._add_proc_task(next(data))

        last_id = None
        while not self.manager.empty():
            for d in data:
                if len(self._task_active) >= self._max_active:
                    break
                self._add_proc_task(d)

            t = self.wait(5)
            if t:
                if t.successful():
                    last_id = t.id
                    self._tasks_to_accumulate.append(t.id)
                    self._add_accum_tasks()
                else:
                    print("-", t.output)
                    print(t.std_output)

        if last_id:
            with lz4.frame.open(f"out_{id(self)}", "rb") as fp:
                final_result = cloudpickle.load(fp)

            return final_result


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


