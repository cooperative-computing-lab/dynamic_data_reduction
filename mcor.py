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

results_dir = "/cephfs/disc2/users/btovar/cortado"


def source_preprocess(dataset_info, **source_args):
    def file_info_preprocess(file_info):
        import math
        file_chunk_size = source_args.get("file_step_size",  100000)
        num_entries = file_info["num_entries"]
        chunk_adapted = math.ceil(num_entries / math.ceil(num_entries / file_chunk_size))
        start = 0
        while start < num_entries:
            end = min(start + chunk_adapted, num_entries)
            chunk_info = {
                "file": file_info["file"],
                "object_path": "Events",
                "entry_start": start,
                "entry_stop": end,
                "num_entries": end - start,
                "metadata": file_info["metadata"],
            }
            yield (chunk_info, end - start)
            start = end

    for file_info in dataset_info["files"]:
        yield from file_info_preprocess(file_info)


def source_postprocess(chunk_info, **source_args):
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

    cores = source_args.get("cores", int(os.environ.get("CORES", 1)))
    cores_factor = 1

    num_entries = chunk_info["num_entries"]

    start = chunk_info["entry_start"]
    end = chunk_info["entry_stop"]
    steps = [start]

    for step in step_lengths(num_entries, cores * cores_factor):
        if step < 1:
            break
        start += step
        steps.append(start)
    assert start == end

    chunk_info["metadata"]["cores"] = cores
    chunk_info["metadata"]["cores_factor"] = cores_factor

    steps = [chunk_info["entry_start"], chunk_info["entry_stop"]]

    d = {
        chunk_info["file"]: {
            "object_path": chunk_info["object_path"],
            "metadata": chunk_info["metadata"],
            "steps": steps
        }
    }

    events = NanoEventsFactory.from_root(
        d,
        schemaclass=NanoAODSchema,
        uproot_options={"timeout": 300},
        metadata=dict(chunk_info["metadata"]),
    )

    return events.events()


def skimmer(events):
    # import random

    # if random.random() < 0.2:
    #     raise Exception("test failure")

    import cortado.modules.skim_tools as skim_tools
    skimmed = skim_tools.make_skimmed_events(events)
    skimmed = skim_tools.uproot_writeable(skimmed)
    # skimmed = skimmed.repartition(n_to_one=1_000)  # Comment for now, see https://github.com/dask-contrib/dask-awkward/issues/509
    return skimmed


def result_postprocess(processor_name, dataset_name, skim):
    if skim is not None:
        dir = f"{results_dir}/{processor_name}/"
        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

        ak.to_parquet(
            skim,
            f"{dir}/{dataset_name}.parquet"
        )


def accumulator(a, b):
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
    new_data = {}

    for (i, (ds_name, ds_specs)) in enumerate(data.items()):
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
    with open("samples_paper_preprocessed.json", "r") as f:
        data = json.load(f)

    data = coffea_preprocess_to_dynmapred(data)

    mgr = vine.Manager(port=[9128, 9129], name=f"{getpass.getuser()}-cortado-dynmapred", staging_path=f"/tmp/{getpass.getuser()}")
    mgr.tune("hungry-minimum", 1)
    mgr.enable_monitoring(watchdog=False)
    # mgr.set_password_file("vine.password")

    dmr = DynMapReduce(
        mgr,
        source_preprocess=source_preprocess,
        source_postprocess=source_postprocess,
        processors={
            "skimmer": skimmer,
        },
        result_postprocess=result_postprocess,
        accumulator=accumulator,
        accumulation_size=10,
        file_replication=4,
        max_tasks_active=4000,
        max_task_retries=50,
        checkpoint_accumulations=False,
        x509_proxy=f"/tmp/x509up_u{os.getuid()}",
        checkpoint_fn=checkpoint_fn,
        extra_files=[],
        resources_processing={"cores": 1},
        resources_accumualting={"cores": 1},
        results_directory=f"{results_dir}/raw/",
        data=data,
    )

    result = dmr.compute()
    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)
    pprint.pprint(result)
