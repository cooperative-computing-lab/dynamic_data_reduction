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


# Read and input file and return the lines
def read_file(filename):
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content


# Print timing info in s and m
def pretty_print_time(t0, t1, tag, indent=" " * 4):
    dt = t1 - t0
    print(f"{indent}{tag}: {round(dt,3)}s  ({round(dt/60,3)}m)")


if __name__ == "__main__":

    ############ Parse input args ############

    parser = argparse.ArgumentParser()
    parser.add_argument("sample_cfg_name", help="The name of the cfg file")
    parser.add_argument(
        "--executor",
        "-x",
        default="taskvine",
        help="Which executor to use",
        choices=LST_OF_KNOWN_EXECUTORS,
    )
    parser.add_argument(
        "--output", "-o", default=None, help="Location for output file"
    )
    args = parser.parse_args()

    input = args.sample_cfg_name
    output = args.output
    if not output:
        noext = os.path.splitext(input)
        output = f"{input}_preprocessed.json"

    # Check that inputs are good
    # Check that if on UF login node, we're using TaskVine
    hostname = socket.gethostname()
    if "login" in hostname:
        # We are on a UF login node, better be using TaskVine
        # Note if this ends up catching more than UF, can also check for "login"&"ufhpc" in name
        if args.executor == "local":
            raise Exception(
                f"\nError: We seem to be on a UF login node ({hostname}). If running from here, do not run locally."
            )

    # ----- Get list of files from the input jsons -----
    # Get the prefix and json names from the cfg file
    # This is quite brittle
    if args.sample_cfg_name.endswith(".cfg"):
        prefix = ""
        json_lst = []
        lines = read_file(args.sample_cfg_name)
        for line in lines:
            if line.startswith("#"):
                continue
            if line == "":
                continue
            elif line.startswith("prefix:"):
                prefix = line.split()[1]
            else:
                if "#" in line:
                    # Ignore trailing comments, this file parsing is so brittle :(
                    json_lst.append(line.split("#")[0].strip())
                else:
                    json_lst.append(line)
    elif args.sample_cfg_name.endswith(".json"):
        # It seems we have been passed a single json instead of a config file with a list of jsons
        prefix = ""
        json_lst = [args.sample_cfg_name]
    else:
        raise Exception("Unknown input type")

    # Build a sample dict with all info in the jsons
    samples_dict = {}
    for json_name in json_lst:
        with open(json_name) as jf:
            jf_loaded = json.load(jf)
            if jf_loaded["files"] == []:
                print(f"Empty file: {json_name}")
            samples_dict[json_name] = jf_loaded

    # Get and print some summary info about the files to be processed
    print(f"\nInformation about samples to be processed ({len(samples_dict)} total):")
    total_events = 0
    total_files = 0
    total_size = 0
    have_events_and_sizes = True
    # Get the info across the samples_dict
    for ds_name in samples_dict:
        nfiles = len(samples_dict[ds_name]["files"])
        total_files += nfiles
        if "nevents" in samples_dict[ds_name] and "size" in samples_dict[ds_name]:
            # Only try to get this info if it's in the dict
            nevents = samples_dict[ds_name]["nevents"]
            size = samples_dict[ds_name]["size"]
            total_events += nevents
            total_size += size
            print(f"    Name: {ds_name} ({nevents} events)")
        else:
            print(f"    Name: {ds_name}")
            have_events_and_sizes = False
    # Print out totals
    print(f"    Total files: {total_files}")
    if have_events_and_sizes:
        print(f"    Total events: {total_events}")
        print(f"    Total size: {total_size}\n")

    # Make the dataset object the processor wants
    dataset_dict = {}
    for json_path in samples_dict.keys():
        # Drop the .json from the end to get a name
        tag = json_path.split("/")[-1][:-5]
        # Prepend the prefix to the filenames and fill into the dataset dict
        dataset_dict[tag] = {}
        dataset_dict[tag]["files"] = {}
        for filename in samples_dict[json_path]["files"]:
            fullpath = prefix + filename
            dataset_dict[tag]["files"][fullpath] = "Events"

    # print(f"\nDataset dict:\n{dataset_dict}\n")

    ############ Set up DaskVine stuff ############

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
        proxy = m.declare_file(f"/tmp/x509up_u{os.getuid()}", cache=True)

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
        file_exceptions=(Exception,)
    )

    bad = set(dataset_all.keys()) - set(dataset_runnable.keys())
    if bad:
        print("bad files:")
        for fname in bad:
            print(fname)

        print(bad_filesdataset_all - dataset_runnable)
    t_after_preprocess = time.time()
    pretty_print_time(t_after_setup, t_after_preprocess, "Preprocess time", "")

    with open(output, "w") as f:
        json.dump(dataset_runnable, f, indent=4)
