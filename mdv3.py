#! /usr/bin/env python

from dynmapred import DynMapReduce
import ndcctools.taskvine as vine
import cloudpickle
import pprint


def source_preprocess(file_info, **source_args):
    import math

    file_chunk_size = source_args.get("file_step_size", 50000)
    file_chunk_size = source_args.get("file_step_size", 300000)

    source_root = "root://hactar01.crc.nd.edu//store/user/cmoore24/samples/"
    source_ceph = "/cms/cephfs/data/store/user/cmoore24/samples"

    file_info["file"] = file_info["file"].replace(source_ceph, source_root)
    num_entries = file_info["num_entries"]

    chunk_adapted = math.ceil(num_entries / math.ceil(num_entries / file_chunk_size))
    start = 0
    while start < num_entries:
        end = min(start + chunk_adapted, num_entries)
        d = {
            "file": file_info["file"],
            "object_path": "Events",
            "entry_start": start,
            "entry_stop": end,
            "metadata": file_info["metadata"],
        }
        yield d
        start = end


def source_postprocess(chunk_info, **source_args):
    from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
    import math
    import os

    cores = source_args.get("cores", int(os.environ.get("CORES", 1)))
    num_entries = chunk_info["entry_stop"] - chunk_info["entry_start"]
    step = source_args.get("chunk_step_size", math.ceil(num_entries / cores))

    steps = []
    start = chunk_info["entry_start"]

    while start < chunk_info["entry_stop"]:
        end = min(start + step, chunk_info["entry_stop"])
        steps.append([start, end])
        start = end

    d = {
        chunk_info["file"]: {
            "object_path": chunk_info["object_path"],
            "metadata": chunk_info["metadata"],
            "steps": steps,
        }
    }

    events = NanoEventsFactory.from_root(
        d,
        schemaclass=PFNanoAODSchema,
        uproot_options={"timeout": 3000},
        metadata=dict(chunk_info["metadata"]),
    )

    return events.events()


def btag(events):
    import hist.dask as dhist
    import awkward as ak

    label = events.metadata["label"]
    fatjet = events.FatJet

    if "QCD" in label:
        print("background")
        cut = (
            (fatjet.pt > 300)
            & (fatjet.msoftdrop > 110)
            & (fatjet.msoftdrop < 140)
            & (abs(fatjet.eta) < 2.5)
        )  # & (fatjet.btagDDBvLV2 > 0.20)

    else:
        print("signal")
        genhiggs = events.GenPart[
            (events.GenPart.pdgId == 25)
            & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
        ]
        parents = events.FatJet.nearest(genhiggs, threshold=0.1)
        higgs_jets = ~ak.is_none(parents, axis=1)

        cut = (
            (fatjet.pt > 300)
            & (fatjet.msoftdrop > 110)
            & (fatjet.msoftdrop < 140)
            & (abs(fatjet.eta) < 2.5)
        ) & (
            higgs_jets
        )  # & (fatjet.btagDDBvLV2 > 0.20)

    boosted_fatjet = fatjet[cut]
    boosted_fatjet.constituents.pf["pt"] = (
        boosted_fatjet.constituents.pf.pt * boosted_fatjet.constituents.pf.puppiWeight
    )
    btag = dhist.Hist.new.Reg(40, 0, 1, name="Btag", label="Btag").Weight()
    btag.fill(Btag=ak.flatten(boosted_fatjet.btagDDBvLV2))

    return btag


def accumulator(a, b):
    a += b
    return a


def checkpoint_fn(t):
    c = False
    if t.checkpoint_distance > 10:
        c = True

    cumulative = t.cumulative_exec_time
    if cumulative > 3600:
        c = True

    if c:
        print(
            f"checkpointing {t.description()} {t.checkpoint_distance} {t.exec_time} {t.cumulative_exec_time}"
        )

    return c


if __name__ == "__main__":
    data_root = "/afs/crc.nd.edu/user/b/btovar/src/dynmapred/data/samples"
    data = {
        "some dataset A": [
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset A"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset A"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset A"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset A"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset A"},
            },
        ],
        "some dataset B": [
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset B"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset B"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset B"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset B"},
            },
            {
                "file": f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
                "object_path": "Events",
                "metadata": {"dataset": "some dataset B"},
            },
        ],
    }

    with open("preprocessed/dv3_preprocessed.pkl", "rb") as f:
        data = cloudpickle.load(f)

        # data = {"hbb": data["hbb"]}

    mgr = vine.Manager(port=0, name="btovar-dynmapred", staging_path="/tmp/btovar")
    dmr = DynMapReduce(
        mgr,
        source_preprocess=source_preprocess,
        source_postprocess=source_postprocess,
        processors={"btag_1": btag, "btag_2": btag},
        accumulator=accumulator,
        accumulation_size=10,
        max_tasks_active=1200,
        max_sources_per_dataset=None,
        max_task_retries=5,
        checkpoint_accumulations=False,
        x509_proxy="x509up_u196886",
        checkpoint_fn=checkpoint_fn,
    )

    result = dmr.compute(data)

    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)
    pprint.pprint(result)
