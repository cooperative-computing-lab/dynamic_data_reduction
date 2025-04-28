#! /usr/bin/env python

from ddr import DynamicDataReduction, DataT, ProcT, ResultT
import ndcctools.taskvine as vine
import awkward as ak
import json
import pathlib
import getpass
import os
from functools import partial
from typing import Any, Callable, Hashable, Mapping, List, Optional, TypeVar, Self

from coffea.nanoevents import NanoAODSchema

def make_source_postprocess(schema, uproot_options):
    """ Called at the worker. Rechunks chunk specification to use many cores,
    and created a NanoEventsFactory per chunk. """

    def source_postprocess(chunk_info, **kwargs):
        from coffea.nanoevents import NanoEventsFactory
        import os

        def step_lengths(n, c):
            c = max(1, c)
            n = max(1, n)

            # s is the base number of events per CPU
            s = n // c

            # r is how many CPUs need to handle s+1 events
            r = n % c

            return [s + 1] * r + [s] * (c - r)

        cores = int(os.environ.get("CORES", 1))
        num_entries = chunk_info["num_entries"]

        start = chunk_info["entry_start"]
        end = chunk_info["entry_stop"]
        steps = [start]

        for step in step_lengths(num_entries, cores):
            if step < 1:
                break
            start += step
            steps.append(start)
        assert start == end

        chunk_info["metadata"]["cores"] = cores
        steps = [chunk_info["entry_start"], chunk_info["entry_stop"]]

        d = {
            chunk_info["file"]: {
                "object_path": chunk_info["object_path"],
                "num_entries": chunk_info["entry_stop"] - chunk_info["entry_start"],
                "steps": steps,
                "metadata": chunk_info["metadata"],
            }
        }
        events = NanoEventsFactory.from_root(
            d,
            schema=schema,
            uproot_options=uproot_options,
            metadata=chunk_info["metadata"],
        )

        return events.events()
    
    return source_postprocess


def make_source_preprocess(step_size, object_path):
    def source_preprocess(dataset_info, **source_args):
        """ Called at the manager. It splits single file specifications into multiple chunks specifications. """

        def file_info_preprocess(file_info):
            import math
            num_entries = file_info["num_entries"]

            chunk_adapted = math.ceil(num_entries / math.ceil(num_entries / step_size))
            start = 0
            while start < num_entries:
                end = min(start + chunk_adapted, num_entries)
                chunk_info = {
                    "file": file_info["file"],
                    "object_path": object_path,
                    "entry_start": start,
                    "entry_stop": end,
                    "num_entries": end - start,
                    "metadata": file_info["metadata"],
                }
                yield (chunk_info, chunk_info["num_entries"])
                start = end

        for file_info in dataset_info["files"]:
            yield from file_info_preprocess(file_info)

    return source_preprocess



class CoffeaDynamicDataReduction(DynamicDataReduction):
    def __init__(
        self,
        manager: vine.Manager,
        processors: (
            Callable[[ProcT], ResultT]
            | List[Callable[[ProcT], ResultT]]
            | dict[str, Callable[[ProcT], ResultT]]
        ),
        data: dict[str, dict[str, Any]],
        accumulation_size: int = 10,
        accumulator: Callable[[ResultT, ResultT], ResultT] = None,
        checkpoint_accumulations: bool = False,
        checkpoint_max_distance: int = 3,
        checkpoint_max_time: int = 1800,
        environment: Optional[str] = None,
        extra_files: Optional[list[str]] = None,
        file_replication: int = 3,
        max_task_retries: int = 10,
        max_tasks_active: Optional[int] = None,
        max_tasks_submit_batch: Optional[int] = None,
        processor_args: Optional[Mapping[str, Any]] = None,
        resources_accumualting: Optional[Mapping[str, float]] = None,
        resources_processing: Optional[Mapping[str, float]] = None,
        results_directory: str = "results",
        result_postprocess: Optional[Callable[[str, str, str, ResultT], Any]] = None,
        graph_output_file: bool = True,

        x509_proxy: Optional[str] = None,

        schema: Optional[Any] = NanoAODSchema,
        step_size: int = 100_000,
        object_path: str = "Events",
        uproot_options: Optional[Mapping[str, Any]] = None,
        ):

        super().__init__(
            manager=manager,
            processors=processors,
            data=self.from_coffea_preprocess(data),
            accumulation_size=accumulation_size,
            accumulator=accumulator,
            checkpoint_accumulations=checkpoint_accumulations,
            checkpoint_max_distance=checkpoint_max_distance,
            checkpoint_max_time=checkpoint_max_time,
            environment=environment,
            extra_files=extra_files,
            file_replication=file_replication,
            max_task_retries=max_task_retries,
            max_tasks_active=max_tasks_active,
            max_tasks_submit_batch=max_tasks_submit_batch,
            processor_args=processor_args,
            resources_accumualting=resources_accumualting,
            resources_processing=resources_processing,
            results_directory=results_directory,
            result_postprocess=result_postprocess,
            graph_output_file=graph_output_file,
            x509_proxy=x509_proxy,

            source_postprocess=make_source_postprocess(schema, uproot_options),
            source_preprocess=make_source_preprocess(step_size, object_path),
        )


@classmethod
def from_coffea_preprocess(cls, data):
    """ Converts coffea style preprocessed data into DynMapReduce data. """
    new_data = {}

    for (i, (ds_name, ds_specs)) in enumerate(reversed(data.items())):
        new_specs = []
        extra_data = dict(ds_specs)
        del extra_data["files"]

        dataset_events = 0
        total_events = 0
        for (j, (filename, file_info)) in enumerate(ds_specs["files"].items()):
            if file_info["num_entries"] < 1:
                continue

            dataset_events += file_info["num_entries"]
            total_events += dataset_events
            d = {"file": filename}
            d.update(file_info)
            d.update(extra_data)

            if "metadata" not in d or d["metadata"] is None:
                d["metadata"] = {}
            d["metadata"]["dataset"] = ds_name
            new_specs.append(d)
        if len(new_specs) > 0:
            new_data[ds_name] = {
                "files": new_specs,
                "size": dataset_events,
            }
    return {"datasets": new_data, "size": total_events}