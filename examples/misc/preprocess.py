#! /usr/bin/env python

from dynmapred import DynMapReduce
import ndcctools.taskvine as vine
import dask.delayed
import cloudpickle
import pathlib


def source_connector(file_info):
    return file_info


@dask.delayed
def preprocess(file_info):
    import uproot

    with uproot.open({file_info["file"]: "Events"}) as f:
        return [
            {
                "file": file_info["file"],
                "metadata": {"label": file_info["label"]},
                "object_path": "Events",
                "steps": [[0, f.num_entries]],
                "num_entries": f.num_entries,
            }
        ]


def accumulate(a, b):
    return a + b


# data_root = "/afs/crc.nd.edu/user/b/btovar/src/dynmapred/data/samples"
# data = {
#     "some dataset A": [
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
#     ],
#     "some dataset B": [
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_11.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_12.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_13.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_14.root",
#         f"{data_root}/flat400/mass100/RunIISummer20UL17PFNANOAODSIM_15.root",
#     ],
# }


try:
    with open("dv3_preprocessed.pkl", "rb") as f:
        datasets = cloudpickle.load(f)
except (IOError, EOFError, FileNotFoundError):
    try:
        with open("dv3_all_files.pkl", "rb") as f:
            all_files = cloudpickle.load(f)
    except (IOError, EOFError, FileNotFoundError):
        source_root = "/cms/cephfs/data/store/user/cmoore24/samples"
        datasets = {
            "hgg": {"path": "hgg", "label": "Hgg"},
            "hbb": {"path": "hbb", "label": "Hbb"},
            "q347": {"path": "qcd/300to470", "label": "QCD_Pt_300to470"},
            "q476": {"path": "qcd/470to600", "label": "QCD_Pt_470to600"},
            "q68": {"path": "qcd/600to800", "label": "QCD_Pt_600to800"},
            "q810": {"path": "qcd/800to1000", "label": "QCD_Pt_800to1000"},
            "q1014": {"path": "qcd/1000to1400", "label": "QCD_Pt_1000to1400"},
            "q1418": {"path": "qcd/1400to1800", "label": "QCD_Pt_1400to1800"},
            "q1824": {"path": "qcd/1800to2400", "label": "QCD_Pt_1800to2400"},
            "q2432": {"path": "qcd/2400to3200", "label": "QCD_Pt_2400to3200"},
            "q32Inf": {"path": "qcd/3200toInf", "label": "QCD_Pt_3200toInf"},
        }
        all_files = {}
        for ds, info in datasets.items():
            all_files[ds] = []
            for path in pathlib.Path(f"{source_root}/{info['path']}").glob("*.root"):
                all_files[ds].append(
                    {"file": str(path.absolute()), "label": info["label"]}
                )
        print(all_files)

        with open("dv3_all_files.pkl", "wb") as f:
            cloudpickle.dump(all_files, f)

        mgr = vine.Manager(port=9129, name="btovar-dynmapred")
        dmr = DynMapReduce(
            mgr,
            source_connector=source_connector,
            processor=preprocess,
            accumulator=accumulate,
            x509_proxy="x509up_u196886",
        )
        result = dmr.compute(all_files)

        with open("dv3_preprocessed.pkl", "wb") as f:
            cloudpickle.dump(result, f)
