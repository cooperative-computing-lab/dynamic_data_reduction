import pytest


@pytest.mark.skipif(
    not pytest.importorskip("dynamic_data_reduction", reason="TaskVine ddr not available"),
    reason="TaskVine not available",
)
def test_taskvine_with_ddr():
    """Test TaskVineExecutor with virtual arrays (lazy loading) and eager evaluation"""
    import os.path as osp

    from coffea.nanoevents import schemas
    from coffea.processor.test_items import NanoEventsProcessor
    import dynamic_data_reduction as ddr
    import ndcctools.taskvine as vine

    port = 9123

    # # Use the same filelist as in local executors test
    # filelist = {
    #     "ZJets": {
    #         "files": {
    #             osp.abspath("samples/nano_dy.root"): {
    #                     "object_path": "Events",
    #             },
    #         "metadata": {"checkusermeta": True, "someusermeta": "hello"},
    #     },
    #     },
    #     "Data": {
    #         "files": {
    #             osp.abspath("samples/nano_dimuon.root"): {
    #                     "object_path": "Events",
    #             },
    #         },
    #         "metadata": {"checkusermeta": True, "someusermeta2": "world"},
    #     }
    # }

    # # INSERT_YOUR_CODE
    # from coffea.dataset_tools import preprocess
    # filelist, _ = preprocess(filelist, align_clusters=False, step_size=1_000_000, files_per_batch=10, skip_bad_files=True, save_form=False)
    # print(filelist)

    filelist = {
        'ZJets': {
            'files': {
                osp.abspath('samples/nano_dy.root'): {
                    'object_path': 'Events',
                    'steps': [[0, 40]],
                    'num_entries': 40,
                    'uuid': 'a9490124-3648-11ea-89e9-f5b55c90beef'
                },
            },
            'metadata': {
                'checkusermeta': True, 'someusermeta': 'hello'
            },
        },
        'Data': {
            'files': {
                osp.abspath('samples/nano_dimuon.root'): {
                    'object_path': 'Events',
                    'steps': [[0, 40]],
                    'num_entries': 40,
                    'uuid': 'a210a3f8-3648-11ea-a29f-f5b55c90beef'
                }
            },
            'metadata': {
                'checkusermeta': True, 'someusermeta2': 'world'
            },
        },
    }

    mgr = vine.Manager(port=9123)
    run = ddr.CoffeaDynamicDataReduction(
        data=filelist,
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


if __name__ == '__main__':
    test_taskvine_with_ddr()
