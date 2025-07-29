import argparse
import time
import socket
import json
import os
from ndcctools.taskvine import DaskVine
from coffea.nanoevents import NanoAODSchema
from coffea.dataset_tools import preprocess
from functools import partial

t_start = time.time()

NanoAODSchema.warn_missing_crossrefs = False

LST_OF_KNOWN_EXECUTORS = ["local", "taskvine"]


def read_file(filename):
    with open(filename) as f:
        return json.load(f)


if __name__ == "__main__":

    ############ Parse input args ############

    parser = argparse.ArgumentParser()
    parser.add_argument("sample_json_name", help="The name of the json file")
    parser.add_argument(
        "--executor",
        "-x",
        default="taskvine",
        help="Which executor to use",
        choices=LST_OF_KNOWN_EXECUTORS,
    )
    parser.add_argument("--output", "-o", default=None, help="Location for output file")
    parser.add_argument(
        "--x509-proxy",
        "-x509",
        default=None,
        help="Path to x509 proxy file",
    )

    args = parser.parse_args()

    input = args.sample_json_name
    output = args.output
    if not output:
        base_name = os.path.splitext(input)[0]
        output = f"{base_name}_preprocessed.json"

    dataset_dict = read_file(args.sample_json_name)

    # Call compute on the skimmed output
    if args.executor == "local":
        print("Will run dask.compute locally")
        executor = None
    elif args.executor == "taskvine":
        print("Will run dask.compute with taskvine")

        m = DaskVine(
            [9123, 9128],
            name=f"coffea-vine-{os.environ['USER']}",
            run_info_path="/tmp/btovar/",
        )
        proxy = m.declare_file(args.x509_proxy, cache=True)

        executor = partial(
            m.get,
            lazy_transfers=True,
            extra_files={proxy: "proxy.pem"},
            env_vars={"X509_USER_PROXY": "proxy.pem"},
            resources={"cores": 1},
            resources_mode="max",
        )

    ############ Run ############

    t_after_setup = time.time()

    # This section is mainly copied from: https://github.com/scikit-hep/coffea/discussions/1100

    # Run preprocess
    print("\nRunning preprocessing..")  # To obtain file splitting
    dataset_runnable, dataset_all = preprocess(
        dataset_dict,
        align_clusters=False,
        step_size=1_000_000,  # You may want to set this to something slightly smaller to avoid loading too much in memory
        files_per_batch=10,
        skip_bad_files=True,
        save_form=True,
        scheduler=executor,
        file_exceptions=(Exception,),
    )

    with open(output, "w") as f:
        json.dump(dataset_runnable, f, indent=4)
