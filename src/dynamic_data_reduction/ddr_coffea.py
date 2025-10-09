#! /usr/bin/env python

from .main import DynamicDataReduction, ProcT, ResultT
import ndcctools.taskvine as vine
from typing import Any, Callable, Mapping, List, Optional

from coffea.nanoevents import NanoAODSchema


def make_source_postprocess(schema, uproot_options):
    """Called at the worker. Rechunks chunk specification to use many cores,
    and created a NanoEventsFactory per chunk."""

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

        # d = {
        #     chunk_info["file"]: {
        #         "object_path": chunk_info["treepath"],
        #         "num_entries": chunk_info["entry_stop"] - chunk_info["entry_start"],
        #         "steps": steps,
        #         "metadata": chunk_info["metadata"],
        #     }
        # }
        # events = NanoEventsFactory.from_root(
        #     d,
        #     schemaclass=schema,
        #     uproot_options=uproot_options,
        #     metadata=chunk_info["metadata"],
        #     mode="virtual"
        # )
        d = dict(chunk_info)
        d["file"] = f'{d["file"]}:{d["treepath"]}'
        del d["file"]
        del d["treepath"]
        del d["num_entries"]

        events = NanoEventsFactory.from_root(
            {chunk_info["file"]: chunk_info["treepath"]},
            **d,
            schemaclass=schema,
            uproot_options=uproot_options,
            mode="virtual",
        )

        return events.events()

    return source_postprocess


def call_processor(processor, events, **processor_args):
    """Wrapper function that calls the processor and materializes virtual arrays if needed."""
    # Call the processor function
    result = processor(events, **processor_args)

    # Materialize virtual arrays in the result if needed
    def materialize_virtual_arrays(obj):
        """Recursively materialize virtual arrays to make them serializable."""
        import awkward as ak
        import numpy as np

        def _materialize_recursive(item):
            try:
                # Try to materialize using ak.materialize
                materialized = ak.materialize(item)
                # Force materialization by accessing the data
                if hasattr(materialized, "layout"):
                    # Access the layout to ensure it's fully materialized
                    _ = materialized.layout
                    if hasattr(materialized.layout, "content"):
                        _ = materialized.layout.content
                # Also try to get the length to force evaluation
                _ = len(materialized)
                return materialized
            except Exception:
                # If it's not an awkward array or materialization fails, handle recursively
                if isinstance(item, dict):
                    return {k: _materialize_recursive(v) for k, v in item.items()}
                elif isinstance(item, (list, tuple)):
                    return type(item)(_materialize_recursive(v) for v in item)
                elif hasattr(item, "__iter__") and not isinstance(
                    item, (str, bytes, np.ndarray)
                ):
                    try:
                        return type(item)(_materialize_recursive(v) for v in item)
                    except Exception:
                        return item
                else:
                    return item

        # Add some debugging information
        try:
            result = _materialize_recursive(obj)
            return result
        except Exception as e:
            import warnings

            warnings.warn(f"Materialization failed: {str(e)}")
            return obj

    # Materialize any virtual arrays in the result
    result = materialize_virtual_arrays(result)

    return result


def make_source_preprocess(step_size, treepath):
    def source_preprocess(dataset_info, **source_args):
        """Called at the manager. It splits single file specifications into multiple chunks specifications."""

        def file_info_preprocess(file_info):
            import math

            num_entries = file_info["num_entries"]

            chunk_adapted = math.ceil(num_entries / math.ceil(num_entries / step_size))
            start = 0
            while start < num_entries:
                end = min(start + chunk_adapted, num_entries)
                chunk_info = {
                    "file": file_info["file"],
                    "treepath": treepath,
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
        accumulator: Optional[Callable[[ResultT, ResultT], ResultT]] = None,
        checkpoint_accumulations: bool = False,
        checkpoint_distance: int = 3,
        checkpoint_time: int = 1800,
        checkpoint_postprocess: Optional[
            Callable[[str, str, str, bool, int, ResultT], Any]
        ] = None,
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
        remote_executor_args: Optional[Mapping[str, Any]] = None,
        x509_proxy: Optional[str] = None,
        schema: Optional[Any] = NanoAODSchema,
        step_size: int = 100_000,
        object_path: str = "Events",
        uproot_options: Optional[Mapping[str, Any]] = None,
        max_datasets: Optional[int] = None,
        max_files_per_dataset: Optional[int] = None,
        skip_datasets: Optional[List[str]] = None,
        resource_monitor: str | bool | None = "measure",
    ):

        # Wrap processors to handle virtual array materialization
        wrapped_processors = self._wrap_processors(processors)

        super().__init__(
            manager=manager,
            processors=wrapped_processors,
            data=self.from_coffea_preprocess(
                data, max_datasets, max_files_per_dataset, skip_datasets
            ),
            accumulation_size=accumulation_size,
            accumulator=accumulator,
            checkpoint_postprocess=checkpoint_postprocess,
            checkpoint_accumulations=checkpoint_accumulations,
            checkpoint_distance=checkpoint_distance,
            checkpoint_time=checkpoint_time,
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
            remote_executor_args=remote_executor_args,
            source_postprocess=make_source_postprocess(schema, uproot_options),
            source_preprocess=make_source_preprocess(step_size, object_path),
            skip_datasets=skip_datasets,
            resource_monitor=resource_monitor,
        )

    def from_coffea_preprocess(
        self, data, max_datasets=None, max_files_per_dataset=None, skip_datasets=None
    ):
        """Converts coffea style preprocessed data into DynMapReduce data."""
        import re

        new_data = {}
        total_events = 0

        for ds_index, (ds_name, ds_specs) in enumerate(data.items()):
            if max_datasets and ds_index >= max_datasets:
                break

            # Skip datasets that match any of the skip_datasets regex patterns
            if skip_datasets:
                should_skip = False
                for pattern in skip_datasets:
                    if re.search(pattern, ds_name):
                        should_skip = True
                        break
                if should_skip:
                    continue

            new_specs = []
            extra_data = dict(ds_specs)
            del extra_data["files"]

            dataset_events = 0
            for ds_files_index, (filename, file_info) in enumerate(
                ds_specs["files"].items()
            ):
                if file_info["num_entries"] < 1:
                    continue

                if max_files_per_dataset and ds_files_index >= max_files_per_dataset:
                    break

                dataset_events += file_info["num_entries"]
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
                total_events += dataset_events

        return {"datasets": new_data, "size": total_events}

    def _wrap_processors(self, processors):
        """Wrap processors to handle virtual array materialization."""
        if isinstance(processors, dict):
            return {
                name: self._wrap_single_processor(proc)
                for name, proc in processors.items()
            }
        elif isinstance(processors, list):
            return [self._wrap_single_processor(proc) for proc in processors]
        else:
            return self._wrap_single_processor(processors)

    def _wrap_single_processor(self, processor):
        """Wrap a single processor function."""

        def wrapped_processor(events, **processor_args):
            return call_processor(processor, events, **processor_args)

        return wrapped_processor
