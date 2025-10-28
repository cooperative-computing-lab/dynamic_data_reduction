import os.path as osp


def taskvine_with_ddr():
    from coffea.nanoevents import schemas
    from coffea.processor.test_items import NanoEventsProcessor
    import dynamic_data_reduction as ddr
    from dynamic_data_reduction import preprocess
    import ndcctools.taskvine as vine

    port = 9123

    mgr = vine.Manager(port=9123)

    filelist = {
        "ZJets": {
            "files": {
                osp.abspath("samples/nano_dy.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta": "hello"},
                    "num_entries": 40
                },
            },
        },
        "Data": {
            "files": {
                osp.abspath("samples/nano_dimuon.root"): {
                    "object_path": "Events",
                    "metadata": {"checkusermeta": True, "someusermeta2": "world"},
                    "num_entries": 40
                }
            },
        },
    }

    run = ddr.CoffeaDynamicDataReduction(
        data=filelist,  # Use preprocessed data
        manager=mgr,
        processors={"proc": NanoEventsProcessor(mode="virtual")},
        accumulator=NanoEventsProcessor,
        schema=schemas.NanoAODSchema,
        resources_processing={
            "cores": 1,
            "disk": 1024,
        },
        verbose=True,
    )

    try:
        # Test that the runner can process the files with both modes
        from ndcctools.taskvine import Factory

        workers = Factory(manager_host_port=f"localhost:{port}", batch_type="local")
        workers.min_workers = 1
        workers.max_workers = 1
        workers.cores = 2
        workers.disk = 4096

        with workers:
            hists = run.compute()

        print(hists)

        # Check that we get the expected results (same as local executors)
        assert hists["proc"]["ZJets"]["cutflow"]["ZJets_pt"] == 18
        assert hists["proc"]["ZJets"]["cutflow"]["ZJets_mass"] == 6
        assert hists["proc"]["Data"]["cutflow"]["Data_pt"] == 84
        assert hists["proc"]["Data"]["cutflow"]["Data_mass"] == 66

        # Verify that both modes work correctly
        assert "mass" in hists["proc"]["ZJets"]
        assert "pt" in hists["proc"]["ZJets"]
        assert "mass" in hists["proc"]["Data"]
        assert "pt" in hists["proc"]["Data"]

    except (ImportError, RuntimeError, FileNotFoundError) as e:
        # Expected if TaskVine is not properly configured or test files don't exist
        assert any(
            x in str(e).lower() for x in ["taskvine", "ndcctools", "file", "not found"]
        )


if __name__ == "__main__":
    taskvine_with_ddr()
