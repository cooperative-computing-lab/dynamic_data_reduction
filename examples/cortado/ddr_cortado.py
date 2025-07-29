#! /usr/bin/env python

from ddr import CoffeaDynamicDataReduction
import ndcctools.taskvine as vine
import json
import pathlib
import pprint
import getpass
import os

from coffea.nanoevents import NanoAODSchema

results_dir = "/cephfs/disc2/users/btovar/cortado"


def isroot_compat(a):
    """Check if array is compatible with ROOT writing"""
    import awkward as ak

    try:
        t = ak.type(a)

        # Reject union types explicitly
        if isinstance(t, ak.types.UnionType):
            return False

        # Simple numeric types
        if isinstance(t, ak.types.NumpyType):
            return True

        # Jagged arrays with numeric content
        # Accept jagged or fixed-size arrays with numeric content (including one level of nesting)
        if isinstance(t, (ak.types.ArrayType, ak.types.ListType, ak.types.RegularType)):
            content = t.content
            if isinstance(content, ak.types.UnionType):
                return False
            if isinstance(content, ak.types.NumpyType):
                return True
            if isinstance(
                content, (ak.types.ArrayType, ak.types.ListType, ak.types.RegularType)
            ):
                if isinstance(content.content, ak.types.UnionType):
                    return False
                if isinstance(content.content, ak.types.NumpyType):
                    return True
        return False
    except:
        return False


def uproot_writeable(events):
    """Restrict to columns that uproot can write compactly"""
    import awkward as ak

    # Start with simple fields (no subfields)
    out_event = events[[x for x in events.fields if not events[x].fields]]

    # Remove parameters from simple fields
    for x in out_event.fields:
        out_event[x] = ak.without_parameters(out_event[x])

    # Process complex fields (collections)
    for bname in events.fields:
        if events[bname].fields:
            compatible_fields = {}
            for n in events[bname].fields:
                cleaned_field = ak.without_parameters(events[bname][n])
                if isroot_compat(cleaned_field):
                    compatible_fields[n] = cleaned_field

            if len(compatible_fields) > 0:
                out_event[bname] = ak.zip(compatible_fields)

    return out_event


def skimmer(events):

    # Some placeholder simple 4l selection
    def make_skimmed_events(events):
        import awkward as ak

        ele = events.Electron
        muo = events.Muon
        nlep = ak.num(ele) + ak.num(muo)
        mask = nlep >= 4
        # print("e+m", nlep.compute())

        return events[mask]

    skimmed = make_skimmed_events(events)
    skimmed = uproot_writeable(skimmed)

    return skimmed


def skimmer_from_module(events):
    """Executes at the worker. The actual computation.
    It receives the event.events() from source_postprocess."""
    import cortado.modules.skim_tools_plain as skim_tools

    # import cortado.modules.skim_tools as skim_tools

    skimmed = skim_tools.make_skimmed_events(events)
    skimmed = skim_tools.uproot_writeable(skimmed)

    return skimmed


def result_postprocess(processor_name, dataset_name, results_dir, skim):
    """Executes at the manager. Saves python object into parquet file."""
    import awkward as ak

    if skim is not None:
        dir = f"{results_dir}/{processor_name}/{dataset_name}"
        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
        ak.to_parquet(skim, f"{dir}/output.parquet")

    return None


def checkpoint_postprocess_bad(
    processor_name, dataset_name, results_dir, force, index, skim
):
    """Executes at the manager. Saves python object into root file."""
    import uproot
    import awkward as ak

    def is_root_compat(a):
        if isinstance(a, ak.types.NumpyType):
            return True
        elif isinstance(a, ak.types.ListType) and isinstance(
            a.content, ak.types.NumpyType
        ):
            return True
        else:
            return False

    def uproot_writeable(skim):
        out = skim[list(x for x in skim.fields if not skim[x].fields)]
        for bname in skim.fields:
            if skim[bname].fields:
                d = {
                    str(n): ak.without_parameters(skim[bname][n])
                    for n in skim[bname].fields
                    if is_root_compat(skim[bname][n])
                }
                if len(d) > 0:
                    out[bname] = ak.zip(d)
        return out

    if skim is not None:
        dir = f"{results_dir}/{processor_name}/{dataset_name}"
        file_name = f"{dir}/output_{index:05d}.root"
        print(f"Writing{file_name}!")

        pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

        with uproot.recreate(file_name) as f:
            f["Events"] = uproot_writeable(skim)

        print(f"Wrote {file_name}!")

        # uproot.dask_write(
        #     dak.from_awkward(skim, npartitions=1),
        #     destination=dir,
        #     prefix=dataset_name,
        #     compute=True,
        #     tree_name="Events",
        # ).compute()

    # since we saved the file, we don't need to accumualte this result upstream anymore
    return None


def ak_to_root(
    destination,
    array,
    tree_name="Events",
    compression="zlib",
    compression_level=1,
    title="",
    counter_name=lambda counted: "n" + counted,
    field_name=lambda outer, inner: inner if outer == "" else outer + "_" + inner,
    initial_basket_capacity=10,
    resize_factor=10.0,
    storage_options=None,
):
    import uproot

    if compression in ("LZMA", "lzma"):
        compression_code = uproot.const.kLZMA
    elif compression in ("ZLIB", "zlib"):
        compression_code = uproot.const.kZLIB
    elif compression in ("LZ4", "lz4"):
        compression_code = uproot.const.kLZ4
    elif compression in ("ZSTD", "zstd"):
        compression_code = uproot.const.kZSTD
    else:
        msg = f"unrecognized compression algorithm: {compression}. Only ZLIB, LZMA, LZ4, and ZSTD are accepted."
        raise ValueError(msg)

    with uproot.recreate(
        destination,
        compression=uproot.compression.Compression.from_code_pair(
            compression_code, compression_level
        ),
        **(storage_options or {}),
    ) as out_file:

        branch_types = {name: array[name].type for name in array.fields}

        out_file.mktree(
            tree_name,
            branch_types,
            title=title,
            counter_name=counter_name,
            field_name=field_name,
            initial_basket_capacity=initial_basket_capacity,
            resize_factor=resize_factor,
        )

        out_file[tree_name].extend({name: array[name] for name in array.fields})

    return None


def checkpoint_postprocess_root(
    skim, results_dir, processor_name, dataset_name, size, force
):
    """Executes at the manager. Saves python object into root file."""
    import uuid
    import awkward as ak

    if not force and size < 1_000:
        return True

    print(f"Applying checkpoint postprocess for {processor_name}_{dataset_name} {size}")
    dir = f"{results_dir}/{processor_name}/{dataset_name}"
    filename = f"output-{uuid.uuid4()}.root"
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
    ak_to_root(f"{dir}/{filename}", skim)

    # since we saved the file, we don't need to accumualte this result upstream anymore
    return False


def checkpoint_postprocess_parquet(
    skim, results_dir, processor_name, dataset_name, size, force
):
    """Executes at the manager. Saves python object into root file."""
    import uuid
    import awkward as ak

    if not force and size < 1_000:
        return True

    print(f"Applying checkpoint postprocess for {processor_name}_{dataset_name} {size}")
    dir = f"{results_dir}/{processor_name}/{dataset_name}"
    filename = f"output-{uuid.uuid4()}.parquet"
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
    ak.to_parquet(skim, f"{dir}/{filename}")

    # since we saved the file, we don't need to accumualte this result upstream anymore
    return False


def accumulator(a, b, **kwargs):
    """Executes at the worker. Merges two awkward arrays
    from independent skim results."""
    r = None
    if a is None:
        r = b
    elif b is None:
        r = a
    else:
        import awkward as ak

        r = ak.concatenate([a, b], axis=0, mergebool=True)

        # Apply filtering to remove union types
        r = uproot_writeable(r)

    return r


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run DynMapReduce processing")
    parser.add_argument(
        "--accumulation-size", type=int, default=10, help="Accumulation size"
    )
    parser.add_argument(
        "--checkpoint-accumulations",
        type=bool,
        default=False,
        help="Checkpoint accumulations",
    )
    parser.add_argument(
        "--checkpoint-distance", type=int, default=3, help="Checkpoint distance"
    )
    parser.add_argument(
        "--checkpoint-time", type=int, default=1800, help="Checkpoint time"
    )
    parser.add_argument(
        "--cores", type=int, default=1, help="Number of cores per worker"
    )
    parser.add_argument(
        "--file-replication", type=int, default=4, help="File replication factor"
    )
    parser.add_argument(
        "--preprocessed-file",
        type=str,
        required=True,
        help="Input JSON file with datasets information",
    )
    parser.add_argument(
        "--manager-name",
        type=str,
        default=f"{getpass.getuser()}-cortado-dynmapred",
        help="Vine manager name",
    )
    parser.add_argument(
        "--max-task-retries", type=int, default=10, help="Maximum task retries"
    )
    parser.add_argument(
        "--max-tasks-active", type=int, default=4000, help="Maximum active tasks"
    )
    parser.add_argument(
        "--max-datasets",
        type=int,
        default=None,
        help="Maximum number of datasets to process",
    )
    parser.add_argument(
        "--max-files-per-dataset",
        type=int,
        default=None,
        help="Maximum number of files per dataset to process",
    )
    parser.add_argument(
        "--port-range",
        type=str,
        default="9128:9129",
        help="Vine manager port range (colon-separated)",
    )
    parser.add_argument("--prefix", type=str, default="", help="Prefix for input files")
    parser.add_argument(
        "--results-dir",
        type=str,
        required=True,
        default=results_dir,
        help="Directory for results",
    )
    parser.add_argument(
        "--run-info-path",
        type=str,
        default=f"/tmp/{getpass.getuser()}",
        help="Logs and staging path",
    )
    parser.add_argument(
        "--step-size",
        type=int,
        default=100000,
        help="Number of events to process together.",
    )
    parser.add_argument(
        "--x509-proxy",
        type=str,
        default=f"/tmp/x509up_u{os.getuid()}",
        help="X509 proxy",
    )

    args = parser.parse_args()
    preprocessed_file = args.preprocessed_file

    with open(preprocessed_file, "r") as f:
        data = json.load(f)

    port_range = [int(p) for p in args.port_range.split(":")]
    mgr = vine.Manager(
        port=port_range,
        name=f"{getpass.getuser()}x-cortado-dynmapred",
        run_info_path=args.run_info_path,
    )
    mgr.tune("hungry-minimum", 1)
    # mgr.tune("wait-for-workers", 50)
    mgr.enable_monitoring(watchdog=False)

    # Check if the X509 proxy file exists
    x509_proxy = args.x509_proxy
    if not os.path.exists(args.x509_proxy):
        print(
            f"Warning: X509 proxy file {args.x509_proxy} does not exist. Setting to None."
        )
        x509_proxy = None

    ddr = CoffeaDynamicDataReduction(
        mgr,  # taskvine manager
        data=data,  # json, preprocessed from regular coffea: dataset, file, and num_entries
        processors={
            "skimmer": skimmer,
        },
        step_size=args.step_size,
        schema=NanoAODSchema,
        uproot_options={"timeout": 600, "xrootdtimeout": 600},
        remote_executor_args={"scheduler": "threads"},
        # result_postprocess=result_postprocess,
        checkpoint_postprocess=checkpoint_postprocess_root,
        accumulator=accumulator,
        checkpoint_accumulations=args.checkpoint_accumulations,
        checkpoint_distance=args.checkpoint_distance,
        checkpoint_time=args.checkpoint_time,
        x509_proxy=x509_proxy,
        accumulation_size=args.accumulation_size,
        file_replication=args.file_replication,
        max_tasks_active=args.max_tasks_active,
        max_task_retries=args.max_task_retries,
        max_datasets=args.max_datasets,
        max_files_per_dataset=args.max_files_per_dataset,
        extra_files=[],
        resources_processing={"cores": args.cores},
        resources_accumualting={"cores": args.cores + 1},
        results_directory=f"{args.results_dir}",
    )

    result = ddr.compute()
    pprint.pprint(result)
