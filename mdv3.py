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


def make_processor(name):
    def processor(events):
        import hist.dask as dhist
        import awkward as ak
        from mdv3_fns import color_ring, d2_calc, d3_calc, n4_calc, u_calc

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

        if "Color_Ring" == name:
            uf_cr = ak.unflatten(
                color_ring(boosted_fatjet), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["color_ring"] = uf_cr
            hcr = dhist.Hist.new.Reg(
                100, 0.5, 4.5, name="color_ring", label="Color_Ring"
            ).Weight()
            fill_cr = ak.fill_none(ak.flatten(boosted_fatjet.color_ring), 0)
            hcr.fill(color_ring=fill_cr)
            return hcr

        elif "Color_Ring_Var" == name:
            uf_cr_var = ak.unflatten(
                color_ring(boosted_fatjet, variant=True),
                counts=ak.num(boosted_fatjet),
            )
            boosted_fatjet["color_ring_var"] = uf_cr_var
            hcr_var = dhist.Hist.new.Reg(
                40, 0, 3, name="color_ring_var", label="Color_Ring_Var"
            ).Weight()
            fill_cr_var = ak.fill_none(ak.flatten(boosted_fatjet.color_ring_var), 0)
            hcr_var.fill(color_ring_var=fill_cr_var)
            return hcr_var

        elif "D2" == name:
            d2 = ak.unflatten(
                d2_calc(boosted_fatjet), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["d2b1"] = d2
            d2b1 = dhist.Hist.new.Reg(40, 0, 3, name="D2B1", label="D2B1").Weight()
            d2b1.fill(D2B1=ak.flatten(boosted_fatjet.d2b1))
            return d2b1

        elif "D3" == name:
            d3 = ak.unflatten(
                d3_calc(boosted_fatjet), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["d3b1"] = d3
            d3b1 = dhist.Hist.new.Reg(40, 0, 3, name="D3B1", label="D3B1").Weight()
            d3b1.fill(D3B1=ak.flatten(boosted_fatjet.d3b1))
            return d3b1

        elif "N4" == name:
            n4 = ak.unflatten(
                n4_calc(boosted_fatjet), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["n4b1"] = n4
            n4b1 = dhist.Hist.new.Reg(40, 0, 35, name="N4B1", label="N4B1").Weight()
            n4b1.fill(N4B1=ak.flatten(boosted_fatjet.n4b1))
            return n4b1

        elif "U1" == name:
            u1 = ak.unflatten(
                u_calc(boosted_fatjet, n=1), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["u1b1"] = u1
            u1b1 = dhist.Hist.new.Reg(
                40, 0, 0.3, name="U1B1", label="U1B1"
            ).Weight()
            u1b1.fill(U1B1=ak.flatten(boosted_fatjet.u1b1))
            return u1b1

        elif "U2" == name:
            u2 = ak.unflatten(
                u_calc(boosted_fatjet, n=2), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["u2b1"] = u2
            u2b1 = dhist.Hist.new.Reg(
                40, 0, 0.05, name="U2B1", label="U2B1"
            ).Weight()
            u2b1.fill(U2B1=ak.flatten(boosted_fatjet.u2b1))
            return u2b1

        elif "U3" == name:
            u3 = ak.unflatten(
                u_calc(boosted_fatjet, n=3), counts=ak.num(boosted_fatjet)
            )
            boosted_fatjet["u3b1"] = u3
            u3b1 = dhist.Hist.new.Reg(
                40, 0, 0.05, name="U3B1", label="U3B1"
            ).Weight()
            u3b1.fill(U3B1=ak.flatten(boosted_fatjet.u3b1))
            return u3b1

        elif "MRatio" == name:
            mass_ratio = boosted_fatjet.mass / boosted_fatjet.msoftdrop
            boosted_fatjet["mass_ratio"] = mass_ratio
            mosm = dhist.Hist.new.Reg(
                40, 0.9, 1.5, name="MRatio", label="MRatio"
            ).Weight()
            mosm.fill(MRatio=ak.flatten(boosted_fatjet.mass_ratio))
            return mass_ratio

        elif "N2" == name:
            cmssw_n2 = dhist.Hist.new.Reg(
                40, 0, 0.5, name="cmssw_n2", label="CMSSW_N2"
            ).Weight()
            cmssw_n2.fill(cmssw_n2=ak.flatten(boosted_fatjet.n2b1))
            return cmssw_n2

        elif "N3" == name:
            cmssw_n3 = dhist.Hist.new.Reg(
                40, 0, 3, name="cmssw_n3", label="CMSSW_N3"
            ).Weight()
            cmssw_n3.fill(cmssw_n3=ak.flatten(boosted_fatjet.n3b1))
            return cmssw_n3

        elif "nConstituents" == name:
            ncons = dhist.Hist.new.Reg(
                40, 0, 200, name="constituents", label="nConstituents"
            ).Weight()
            ncons.fill(constituents=ak.flatten(boosted_fatjet.nConstituents))
            return ncons

        elif "Mass" == name:
            mass = dhist.Hist.new.Reg(
                40, 0, 250, name="mass", label="Mass"
            ).Weight()
            mass.fill(mass=ak.flatten(boosted_fatjet.mass))
            return mass

        elif "SDmass" == name:
            sdmass = dhist.Hist.new.Reg(
                40, 0, 250, name="sdmass", label="SDmass"
            ).Weight()
            sdmass.fill(sdmass=ak.flatten(boosted_fatjet.msoftdrop))
            return sdmass

        elif "Btag" == name:
            btag = dhist.Hist.new.Reg(40, 0, 1, name="Btag", label="Btag").Weight()
            btag.fill(Btag=ak.flatten(boosted_fatjet.btagDDBvLV2))
            return btag

        else:
            raise ValueError(f"{name} is not a valid processor name")

    processor.__name__ = name
    return processor


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
    if t.checkpoint_distance > 20:
        c = True

    cumulative = t.cumulative_exec_time
    if cumulative > 1800:
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
        processors={
            # name: make_processor(name) for name in ["Color_Ring_Var", "Btag", "D3B1"]
            name: make_processor(name) for name in ["Mass", "SDmass", "Btag", "MRatio", "N2", "N3", "nConstituents"]
            # name: make_processor(name) for name in [ "SDmass" ]
        },
        accumulator=accumulator,
        accumulation_size=25,
        file_replication=3,
        max_tasks_active=1200,
        max_sources_per_dataset=None,
        max_task_retries=10,
        checkpoint_accumulations=False,
        x509_proxy="x509up_u196886",
        checkpoint_fn=checkpoint_fn,
        extra_files=["mdv3_fns.py"],
    )

    result = dmr.compute(data)

    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)
    pprint.pprint(result)
