#! /usr/bin/env python

from dynmapred import DynMapReduce
import ndcctools.taskvine as vine
import awkward as ak
import cloudpickle
import json
import pathlib
import pprint
import getpass
import os

from coffea.nanoevents import NanoAODSchema

results_dir = "/cephfs/disc2/users/btovar/cortado"


def source_preprocess(dataset_info, **source_args):
    """ Called at the manager. It splits single file specifications into multiple chunks specifications. """

    def file_info_preprocess(file_info):
        import math
        file_chunk_size = source_args.get("step_size", 100000)
        object_path = source_args.get("object_path", "Events")

        file_chunk_size = source_args["step_size"]
        num_entries = file_info["num_entries"]
        chunk_adapted = math.ceil(num_entries / math.ceil(num_entries / file_chunk_size))
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
            yield (chunk_info, end - start)
            start = end

    for file_info in dataset_info["files"]:
        yield from file_info_preprocess(file_info)


def source_postprocess(chunk_info, **nanoevents_factory_args):
    """ Called at the worker. Rechunks chunk specification to use many cores,
    and created a NanoEventsFactory per chunk. """
    from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
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
            "metadata": chunk_info["metadata"],
            "steps": steps
        }
    }

    nanoevents_factory_args.setdefault("schemaclass", NanoAODSchema)
    nanoevents_factory_args.setdefault("uproot_options", {"timeout": 300})
    nanoevents_factory_args.setdefault("metadata", {})
    nanoevents_factory_args["metadata"].update(chunk_info["metadata"])

    events = NanoEventsFactory.from_root(
        d,
        **nanoevents_factory_args,
    )

    return events.events()


def skimmer(events):
    """ Executes at the worker. The actual computation. It receives the event.events() from source_postprocess. """
    import cortado.modules.skim_tools as skim_tools
    skimmed = skim_tools.make_skimmed_events(events)
    skimmed = skim_tools.uproot_writeable(skimmed)
    # skimmed = skimmed.repartition(n_to_one=1_000)  # Comment for now, see https://github.com/dask-contrib/dask-awkward/issues/509
    return skimmed


def result_postprocess(processor_name, dataset_name, results_dir, skim):
    """ Executes at the manager. Saves python object into parquet file. """
    if skim is not None:
        dir = f"{results_dir}/{processor_name}/"
        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

        ak.to_parquet(
            skim,
            f"{dir}/{dataset_name}.parquet"
        )

        # uproot.write(
        #     f"{dir}/{dataset_name}.root",
        #     skim,
        #     mode="update",
        #     compression=uproot.ZSTD(1),
        # )


def accumulator(a, b):
    """ Executes at the worker. Merges two awkward arrays from independent skim results. """
    import awkward as ak
    return ak.concatenate(a, b, axis=1)


def checkpoint_fn(t):
    c = False
    if t.checkpoint_distance > 3:
        c = True

    cumulative = t.cumulative_exec_time
    if cumulative > 1800:
        c = True

    return c


def coffea_preprocess_to_dynmapred(data):
    """ Executes at the manager. Converts coffea style preprocessed data into DynMapReduce data. """
    new_data = {}

    for (i, (ds_name, ds_specs)) in enumerate(reversed(data.items())):
        # if i > 5:
        #     break
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
            # if j > 2:
            #     break
        if len(new_specs) > 0:
            new_data[ds_name] = {
                "files": new_specs,
                "size": dataset_events,
            }
    return {"datasets": new_data, "size": total_events}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Run DynMapReduce processing')
    parser.add_argument('--accumulation-size', type=int, default=10, help='Accumulation size')
    parser.add_argument('--checkpoint-accumulations', type=bool, default=False, help='Checkpoint accumulations')
    parser.add_argument('--checkpoint-distance', type=int, default=3, help='Checkpoint distance')
    parser.add_argument('--checkpoint-time', type=int, default=1800, help='Checkpoint time')
    parser.add_argument('--cores', type=int, default=1, help='Number of cores per worker')
    parser.add_argument('--file-replication', type=int, default=4, help='File replication factor')
    parser.add_argument('--preprocessed-file', type=str, required=True, help='Input JSON file with datasets information')
    parser.add_argument('--manager-name', type=str, default=f"{getpass.getuser()}-cortado-dynmapred", help='Vine manager name')
    parser.add_argument('--max-task-retries', type=int, default=50, help='Maximum task retries')
    parser.add_argument('--max-tasks-active', type=int, default=4000, help='Maximum active tasks')
    parser.add_argument('--port-range', type=str, default='9128:9129', help='Vine manager port range (colon-separated)')
    parser.add_argument('--prefix', type=str, default='', help='Prefix for input files')
    parser.add_argument('--results-dir', type=str, required=True, default=results_dir, help='Directory for results')
    parser.add_argument('--staging-path', type=str, default=f"/tmp/{getpass.getuser()}", help='Staging path')
    parser.add_argument('--step-size', type=int, default=100000, help='Number of events to process together.')
    parser.add_argument('--x509-proxy', type=str, default=f"/tmp/x509up_u{os.getuid()}", help='X509 proxy')

    args = parser.parse_args()
    preprocessed_file = args.preprocessed_file

    with open(preprocessed_file, "r") as f:
        data = json.load(f)

    data = coffea_preprocess_to_dynmapred(data)

    port_range = [int(p) for p in args.port_range.split(':')]
    mgr = vine.Manager(port=port_range, name=f"{getpass.getuser()}-cortado-dynmapred", staging_path=args.staging_path)
    mgr.tune("hungry-minimum", 1)
    mgr.enable_monitoring(watchdog=False)

    # Check if the X509 proxy file exists
    x509_proxy = args.x509_proxy
    if not os.path.exists(args.x509_proxy):
        print(f"Warning: X509 proxy file {args.x509_proxy} does not exist. Setting to None.")
        x509_proxy = None

    dmr = DynMapReduce(
        mgr,           # taskvine manager
        data=data,     # json, preprocessed from regular coffea: dataset, file, and num_entries

        source_preprocess=source_preprocess,
        source_preprocess_args={"step_size": args.step_size, "object_path": "Events"},

        source_postprocess=source_postprocess,
        source_postprocess_args={"schemaclass": NanoAODSchema, "uproot_options": {"timeout": 300}},

        processors={
            "skimmer": skimmer,
        },

        remote_executor_args={"scheduler": "threads"},

        result_postprocess=result_postprocess,
        accumulator=accumulator,

        checkpoint_accumulations=args.checkpoint_accumulations,
        checkpoint_fn=checkpoint_fn,

        x509_proxy=x509_proxy,

        accumulation_size=args.accumulation_size,
        file_replication=args.file_replication,
        max_tasks_active=args.max_tasks_active,
        max_task_retries=args.max_task_retries,
        extra_files=[],

        resources_processing={"cores": args.cores},
        resources_accumualting={"cores": args.cores},
        results_directory=f"{args.results_dir}/raw/",
    )

    result = dmr.compute()
    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)
    pprint.pprint(result)
