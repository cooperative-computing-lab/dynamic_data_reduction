#! /usr/bin/env python

from dynmapred import DynMapReduce
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
import hist.dask as dhist
import awkward as ak
import ndcctools.taskvine as vine

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


def source_connector(file_info):
    d = {
        file_info["file"]: {"object_path": "Events", "metadata": file_info["metadata"]}
    }
    events = NanoEventsFactory.from_root(
        d,
        schemaclass=PFNanoAODSchema,
        metadata=dict(file_info["metadata"])
    )

    return events.events()


def processor(events):
    fatjet = events.FatJet
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
    return a + b

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

mgr = vine.Manager(port=9123)
dmr = DynMapReduce(mgr, source_connector=source_connector, processor=processor, accumulator=accumulator)
result = dmr.compute(data)

print(result)
