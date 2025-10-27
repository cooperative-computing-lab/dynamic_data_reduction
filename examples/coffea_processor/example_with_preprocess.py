#!/usr/bin/env python3
"""
Example showing how to use the preprocessing function with TaskVine.
"""

import os.path as osp
import ndcctools.taskvine as vine
from dynamic_data_reduction import preprocess, CoffeaDynamicDataReduction
from coffea.nanoevents import schemas
from coffea.processor.test_items import NanoEventsProcessor


def main():

    # Create TaskVine manager
    port = 9123
    manager = vine.Manager(port=port, name="preprocess-example")

    # Example data structure (before preprocessing)
    data = {
        "ZJets": {
            "files": {
                osp.abspath("samples/nano_dy.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta": "hello"},
                },
                "//"
                + osp.abspath("samples/nano_dy.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta": "hello"},
                },
                "///"
                + osp.abspath("samples/nano_dy.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta": "hello"},
                },
            },
            "metadata": {"checkusermeta": True, "someusermeta": "hello"},
        },
        "Data": {
            "files": {
                osp.abspath("samples/nano_dimuon.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta2": "world"},
                }
            },
            "metadata": {"checkusermeta": True, "someusermeta2": "world"},
        },
    }

    print("Original data structure:")
    for dataset_name, dataset_info in data.items():
        print(f"  {dataset_name}:")
        for file_path, file_info in dataset_info["files"].items():
            print(f"    {file_path}: {file_info}")

    # Preprocess the data using TaskVine
    print("\nPreprocessing data with TaskVine...")
    preprocessed_data = preprocess(
        manager=manager,
        data=data,
        tree_name="Events",
        timeout=60,
        max_retries=3,
        show_progress=True,
        batch_size=5,
    )

    print("\nPreprocessed data structure:")
    for dataset_name, dataset_info in preprocessed_data.items():
        print(f"  {dataset_name}:")
        for file_path, file_info in dataset_info["files"].items():
            print(f"    {file_path}: {file_info}")

    # Now use the preprocessed data with CoffeaDynamicDataReduction
    print("\nUsing preprocessed data with CoffeaDynamicDataReduction...")

    run = CoffeaDynamicDataReduction(
        manager=manager,
        processors={"proc": NanoEventsProcessor(mode="virtual")},
        data=preprocessed_data,
        accumulator=NanoEventsProcessor,
        schema=schemas.NanoAODSchema,
        resources_processing={
            "cores": 1,
            "disk": 1024,
        },
        verbose=True,
    )

    # Start workers and run
    from ndcctools.taskvine import Factory

    workers = Factory(manager_host_port=f"localhost:{port}", batch_type="local")
    workers.min_workers = 1
    workers.max_workers = 2
    workers.cores = 2
    workers.disk = 4096

    with workers:
        hists = run.compute()

    print("\nResults:")
    print(hists)


if __name__ == "__main__":
    main()
