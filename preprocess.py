#! /usr/bin/env python

from dynmapred import DynMapReduce
import ndcctools.taskvine as vine
import dask.delayed
import cloudpickle

data_root = "/afs/crc.nd.edu/user/b/btovar/src/dynmapred/data/samples"
data = {
    "some dataset A": [
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
    ],
    "some dataset B": [
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
        f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
    ],
}


def source_connector(file_info):
    return file_info


@dask.delayed
def preprocess(filename):
    import uproot

    with uproot.open({filename: "Events"}) as f:
        return [
            {
                "file": filename,
                "object_path": "Events",
                "steps": [[0, f.num_entries]],
                "num_entries": f.num_entries,
            }
        ]


def accumulate(a, b):
    return a + b


mgr = vine.Manager(port=9123)
dmr = DynMapReduce(
    mgr, source_connector=source_connector, processor=preprocess, accumulator=accumulate
)
result = dmr.compute(data)

with open("preprocessed.pkl", "wb") as f:
    cloudpickle.dump(result, f)
