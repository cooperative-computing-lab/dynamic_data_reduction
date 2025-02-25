#! /usr/bin/env python

from dynmapred import DynMapReduce
import ndcctools.taskvine as vine
import awkward as ak
import cloudpickle
import json
import pathlib
import pprint
import math


def source_preprocess(file_info, **source_args):
    import math

    file_chunk_size = source_args.get("file_step_size", 3000000)
    file_chunk_size = source_args.get("file_step_size", 50000)
    file_chunk_size = source_args.get("file_step_size", 25000)
    file_chunk_size = source_args.get("file_step_size", 1000)
    file_chunk_size = source_args.get("file_step_size", 10000)
    file_chunk_size = source_args.get("file_step_size", 5000)

    dataset = file_info["metadata"]["dataset"]
    if dataset.startswith("ttboosted"):
        file_chunk_size = source_args.get("file_step_size", 1000)
    elif "qcd" in dataset:
        file_chunk_size = source_args.get("file_step_size", 1000)

    source_root = "root://hactar01.crc.nd.edu//store/user/cmoore24/samples/"
    source_vast = "/project01/ndcms/store/user/btovar/samples/"

    file_info["file"] = file_info["file"].replace(source_root, source_vast)
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
            "num_entries": end - start,
            "metadata": file_info["metadata"],
        }
        yield d
        start = end


def source_postprocess(chunk_info, **source_args):
    from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
    import os

    def step_lengths(n, c):
        c = max(1, c)
        n = max(1, n)

        # s is the base number of events per CPU
        s = n // c

        # r is how many CPUs need to handle s+1 events
        r = n % c

        return [s + 1] * r + [s] * (c - r)

    cores = source_args.get("cores", int(os.environ.get("CORES", 1)))
    cores_factor = 2

    num_entries = chunk_info["num_entries"]
    step_size = max(1, math.floor(num_entries / (cores_factor * cores)))
    step_size = math.ceil(num_entries / math.ceil(num_entries / step_size))

    start = chunk_info["entry_start"]
    end = chunk_info["entry_stop"]
    steps = [start]

    for step in step_lengths(num_entries, cores):
        if step < 1:
            break
        start += step
        steps.append(start)
    assert start == end

    d = {
        chunk_info["file"]: {
            "object_path": chunk_info["object_path"],
            "metadata": chunk_info["metadata"],
            "steps": steps
        }
    }

    events = NanoEventsFactory.from_root(
        d,
        schemaclass=PFNanoAODSchema,
        uproot_options={"timeout": 600, "steps_size": cores * 2},
        metadata=dict(chunk_info["metadata"]),
    )

    return events.events()


def make_processor():
    with open("triggers.json", "r") as f:
        triggers = json.load(f)

    def analysis(events):
        import awkward as ak
        import fastjet
        import scipy

        dataset = events.metadata["dataset"]
        print(dataset)

        events["PFCands", "pt"] = events.PFCands.pt * events.PFCands.puppiWeight

        cut_to_fix_softdrop = ak.num(events.FatJet.constituents.pf, axis=2) > 0
        events = events[ak.all(cut_to_fix_softdrop, axis=1)]

        trigger = ak.zeros_like(ak.firsts(events.FatJet.pt), dtype="bool")
        for t in triggers["2017"]:
            if t in events.HLT.fields:
                trigger = trigger | events.HLT[t]
        trigger = ak.fill_none(trigger, False)

        events["FatJet", "num_fatjets"] = ak.num(events.FatJet)

        goodmuon = (
            (events.Muon.pt > 10)
            & (abs(events.Muon.eta) < 2.4)
            & (events.Muon.pfRelIso04_all < 0.25)
            & events.Muon.looseId
        )

        nmuons = ak.sum(goodmuon, axis=1)

        goodelectron = (
            (events.Electron.pt > 10)
            & (abs(events.Electron.eta) < 2.5)
            & (events.Electron.cutBased >= 2)  # events.Electron.LOOSE
        )
        nelectrons = ak.sum(goodelectron, axis=1)

        ntaus = ak.sum(
            (
                (events.Tau.pt > 20)
                & (abs(events.Tau.eta) < 2.3)
                & (events.Tau.rawIso < 5)
                & (events.Tau.idDeepTau2017v2p1VSjet)
                & ak.all(events.Tau.metric_table(events.Muon[goodmuon]) > 0.4, axis=2)
                & ak.all(
                    events.Tau.metric_table(events.Electron[goodelectron]) > 0.4, axis=2
                )
            ),
            axis=1,
        )

        fatjet_limit = 450  # 400

        # onemuon = (nmuons == 1) & (nelectrons == 0) & (ntaus == 0)
        nolepton = (nmuons == 0) & (nelectrons == 0) & (ntaus == 0)

        region = nolepton  # Use this option to let less data through the cuts

        events["btag_count"] = ak.sum(
            events.Jet[(events.Jet.pt > 20) & (abs(events.Jet.eta) < 2.4)].btagDeepFlavB
            > 0.3040,
            axis=1,
        )

        if ("hgg" in dataset) or ("hbb" in dataset):
            print("signal")
            genhiggs = events.GenPart[
                (events.GenPart.pdgId == 25)
                & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
            ]
            parents = events.FatJet.nearest(genhiggs, threshold=0.2)
            higgs_jets = ~ak.is_none(parents, axis=1)
            events["GenMatch_Mask"] = higgs_jets

            fatjetSelect = (
                (events.FatJet.pt > fatjet_limit)
                # & (events.FatJet.pt < 1200)
                & (abs(events.FatJet.eta) < 2.4)
                & (events.FatJet.msoftdrop > 40)
                & (events.FatJet.msoftdrop < 200)
                & (region)
                & (trigger)
            )

        elif ("wqq" in dataset) or ("ww" in dataset):
            print("w background")
            genw = events.GenPart[
                (abs(events.GenPart.pdgId) == 24)
                & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
            ]
            parents = events.FatJet.nearest(genw, threshold=0.2)
            w_jets = ~ak.is_none(parents, axis=1)
            events["GenMatch_Mask"] = w_jets

            fatjetSelect = (
                (events.FatJet.pt > fatjet_limit)
                # & (events.FatJet.pt < 1200)
                & (abs(events.FatJet.eta) < 2.4)
                & (events.FatJet.msoftdrop > 40)
                & (events.FatJet.msoftdrop < 200)
                & (region)
                & (trigger)
            )

        elif ("zqq" in dataset) or ("zz" in dataset):
            print("z background")
            genz = events.GenPart[
                (events.GenPart.pdgId == 23)
                & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
            ]
            parents = events.FatJet.nearest(genz, threshold=0.2)
            z_jets = ~ak.is_none(parents, axis=1)
            events["GenMatch_Mask"] = z_jets

            fatjetSelect = (
                (events.FatJet.pt > fatjet_limit)
                & (abs(events.FatJet.eta) < 2.4)
                & (events.FatJet.msoftdrop > 40)
                & (events.FatJet.msoftdrop < 200)
                & (region)
                & (trigger)
            )

        elif "wz" in dataset:
            print("wz background")
            genwz = events.GenPart[
                ((abs(events.GenPart.pdgId) == 24) | (events.GenPart.pdgId == 23))
                & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
            ]
            parents = events.FatJet.nearest(genwz, threshold=0.2)
            wz_jets = ~ak.is_none(parents, axis=1)
            events["GenMatch_Mask"] = wz_jets

            fatjetSelect = (
                (events.FatJet.pt > fatjet_limit)
                & (abs(events.FatJet.eta) < 2.4)
                & (events.FatJet.msoftdrop > 40)
                & (events.FatJet.msoftdrop < 200)
                & (region)
                & (trigger)
            )

        else:
            print("background")
            fatjetSelect = (
                (events.FatJet.pt > fatjet_limit)
                & (abs(events.FatJet.eta) < 2.4)
                & (events.FatJet.msoftdrop > 40)
                & (events.FatJet.msoftdrop < 200)
                & (region)
                & (trigger)
            )

        events["goodjets"] = events.FatJet[fatjetSelect]
        mask = ~ak.is_none(ak.firsts(events.goodjets))
        events = events[mask]
        ecfs = {}

        jetdef = fastjet.JetDefinition(fastjet.cambridge_algorithm, 0.8)
        pf = ak.flatten(events.goodjets.constituents.pf, axis=1)
        cluster = fastjet.ClusterSequence(pf, jetdef)
        softdrop = cluster.exclusive_jets_softdrop_grooming()
        softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)

        # (2, 3) -> (2, 6)
        for n in range(2, 6):
            for v in range(1, int(scipy.special.binom(n, 2)) + 1):
                for b in range(5, 45, 5):
                    ecf_name = f"{v}e{n}^{b/10}"
                    ecfs[ecf_name] = ak.unflatten(
                        softdrop_cluster.exclusive_jets_energy_correlator(
                            func="generic", npoint=n, angles=v, beta=b / 10
                        ),
                        counts=ak.num(events.goodjets),
                    )
        events["ecfs"] = ak.zip(ecfs)

        if (
            ("hgg" in dataset)
            or ("hbb" in dataset)
            or ("wqq" in dataset)
            or ("ww" in dataset)
            or ("zqq" in dataset)
            or ("zz" in dataset)
            or ("wz" in dataset)
        ):
            skim = ak.zip(
                {
                    # "Color_Ring": events.goodjets.color_ring,
                    "ECFs": events.ecfs,
                    "msoftdrop": events.goodjets.msoftdrop,
                    "pt": events.goodjets.pt,
                    "btag_ak4s": events.btag_count,
                    "pn_HbbvsQCD": events.goodjets.particleNet_HbbvsQCD,
                    "pn_md": events.goodjets.particleNetMD_QCD,
                    "matching": events.GenMatch_Mask,
                },
                depth_limit=1,
            )
        else:

            skim = ak.zip(
                {
                    # "Color_Ring": events.goodjets.color_ring,
                    "ECFs": events.ecfs,
                    "msoftdrop": events.goodjets.msoftdrop,
                    "pt": events.goodjets.pt,
                    "btag_ak4s": events.btag_count,
                    "pn_HbbvsQCD": events.goodjets.particleNet_HbbvsQCD,
                    "pn_md": events.goodjets.particleNetMD_QCD,
                },
                depth_limit=1,
            )

        return skim

    analysis.__name__ = "skimmer"

    return analysis


def accumulator(a, b):
    import awkward as ak
    return ak.concatenate(a, b, axis=1)
    # if not isinstance(a, list):
    #     a = [a]
    # return a.extend(b)


def checkpoint_fn(t):
    c = False
    if t.checkpoint_distance > 2:
        c = True

    cumulative = t.cumulative_exec_time
    if cumulative > 1800:
        c = True

    # if c:
    #     print(
    #         f"checkpointing {t.description()} {t.checkpoint_distance} {t.exec_time} {t.cumulative_exec_time}"
    #     )

    return c


def coffea_preprocess_to_dynmapred(data):
    data_b = {}
    for (i, (k, v)) in enumerate(data.items()):
        # if k.startswith("ttboosted"):
        #     continue
        if i > 0:
            break

        data_b[k] = []
        v_b = dict(v)
        del v_b["files"]

        for (j, (filename, file_info)) in enumerate(v["files"].items()):
            d = {"file": filename}
            d.update(file_info)
            d.update(v_b)

            if "metadata" not in d or d["metadata"] is None:
                d["metadata"] = {}
            d["metadata"]["dataset"] = k

            data_b[k].append(d)
            if j > 0:
                break
    return data_b


if __name__ == "__main__":
    with open("samples_ready_xrootd.json") as f:
        data = json.load(f)

    data = coffea_preprocess_to_dynmapred(data)

    mgr = vine.Manager(port=[9123, 9129], name="btovar-dynmapred", staging_path="/tmp/btovar")
    mgr.tune("hungry-minimum", 1)
    mgr.enable_monitoring(watchdog=False)

    dmr = DynMapReduce(
        mgr,
        source_preprocess=source_preprocess,
        source_postprocess=source_postprocess,
        processors={
            "skimmer": make_processor()
        },
        accumulator=accumulator,
        accumulation_size=10,
        file_replication=3,
        max_tasks_active=2000,
        max_sources_per_dataset=None,
        max_task_retries=20,
        checkpoint_accumulations=False,
        # x509_proxy="x509up_u196886",
        checkpoint_fn=checkpoint_fn,
        # extra_files=["mdv3_fns.py"],
        resources_processing={"cores": 24},
        resources_accumualting={"cores": 1},
    )

    result = dmr.compute(data)
    with open("result_ac.pkl", "wb") as fw:
        cloudpickle.dump(result, fw)

    for p, data in result.items():
        for dataset, skim in data.items():
            if skim is not None:
                dir = f"/scratch365/btovar/ecf_calculator_output/{dataset}/"
                pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
                ak.to_parquet(
                    skim,
                    f"{dir}/output.parquet"
                )

    pprint.pprint(result)
