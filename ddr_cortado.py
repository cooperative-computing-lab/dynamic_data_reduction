#! /usr/bin/env python

from ddr_coffea import CoffeaDynamicDataReduction
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
    parser.add_argument('--run-info-path', type=str, default=f"/tmp/{getpass.getuser()}", help='Logs and staging path')
    parser.add_argument('--step-size', type=int, default=100000, help='Number of events to process together.')
    parser.add_argument('--x509-proxy', type=str, default=f"/tmp/x509up_u{os.getuid()}", help='X509 proxy')

    args = parser.parse_args()
    preprocessed_file = args.preprocessed_file

    with open(preprocessed_file, "r") as f:
        data = json.load(f)

    port_range = [int(p) for p in args.port_range.split(':')]
    mgr = vine.Manager(port=port_range, name=f"{getpass.getuser()}-cortado-dynmapred", run_info_path=args.run_info_path)
    mgr.tune("hungry-minimum", 1)
    mgr.enable_monitoring(watchdog=False)

    # Check if the X509 proxy file exists
    x509_proxy = args.x509_proxy
    if not os.path.exists(args.x509_proxy):
        print(f"Warning: X509 proxy file {args.x509_proxy} does not exist. Setting to None.")
        x509_proxy = None

    ddr = CoffeaDynamicDataReduction(
        mgr,           # taskvine manager
        data=data,     # json, preprocessed from regular coffea: dataset, file, and num_entries

        processors={
            "skimmer": skimmer,
        },

        step_size=args.step_size,
        schema=NanoAODSchema,
        uproot_options={"timeout": 300},

        remote_executor_args={"scheduler": "threads"},

        result_postprocess=result_postprocess,
        accumulator=accumulator,

        checkpoint_accumulations=args.checkpoint_accumulations,
        checkpoint_distance=args.checkpoint_distance,
        checkpoint_time=args.checkpoint_time,

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

    result = ddr.compute()
    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)
    pprint.pprint(result)
