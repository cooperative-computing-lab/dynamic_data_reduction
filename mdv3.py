#! /usr/bin/env python

from dynmapred import DynMapReduce
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
import hist.dask as dhist
import awkward as ak
import ndcctools.taskvine as vine
import cloudpickle

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
    ]
}

with open("dv3_preprocessed.pkl", "rb") as f:
    data = cloudpickle.load(f)

    # data = {"hbb": data["hbb"]}


def source_connector(file_info):
    source_root = "root://hactar01.crc.nd.edu//store/user/cmoore24/samples/"
    source_ceph = "/cms/cephfs/data/store/user/cmoore24/samples"
    file_info["file"] = file_info["file"].replace(source_ceph, source_root)

    d = {
        file_info["file"]: {
            "object_path": "Events",
        }
    }

    events = NanoEventsFactory.from_root(
        d,
        schemaclass=PFNanoAODSchema,
        uproot_options={"timeout": 3000},
        metadata=dict(file_info["metadata"])
    )

    return events.events()


def processor(events):
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
        boosted_fatjet.constituents.pf.pt
        * boosted_fatjet.constituents.pf.puppiWeight
    )
    btag = dhist.Hist.new.Reg(40, 0, 1, name="Btag", label="Btag").Weight()
    btag.fill(Btag=ak.flatten(boosted_fatjet.btagDDBvLV2))

    return btag


def accumulator(a, b):
    a += b
    return a

# import dynmapred
#
# print(
#     accumulator(
#         dynmapred.wrap_processing(processor, source_connector(data[0])),
#         dynmapred.wrap_processing(processor, source_connector(data[1])),
#     )
# )
#
# import sys
# sys.exit(0)


mgr = vine.Manager(port=0, name="btovar-dynmapred")
dmr = DynMapReduce(
    mgr,
    source_connector=source_connector,
    processor=processor,
    accumulator=accumulator,
    accumulation_size=20,
    max_tasks_active=1000,
    #max_tasks_per_dataset=8,
    x509_proxy="x509up_u196886",
)

result = dmr.compute(data)

with open("result_ac.pkl", "wb") as f:
    cloudpickle.dump(result, f)

import pprint
pprint.pprint(result)
