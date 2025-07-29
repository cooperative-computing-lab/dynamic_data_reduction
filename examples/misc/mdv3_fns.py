import awkward as ak
import fastjet
import coffea.nanoevents.methods.vector as vector
import numpy as np


def color_ring(fatjet, variant=False):
    pf = ak.flatten(fatjet.constituents.pf, axis=1)
    jetdef = fastjet.JetDefinition(fastjet.antikt_algorithm, 0.2)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    subjets = cluster.inclusive_jets()
    vec = ak.zip(
        {
            "x": subjets.px,
            "y": subjets.py,
            "z": subjets.pz,
            "t": subjets.E,
        },
        with_name="LorentzVector",
        behavior=vector.behavior,
    )
    vec = ak.pad_none(vec, 3)
    vec["norm3"] = np.sqrt(vec.dot(vec))
    vec["idx"] = ak.local_index(vec)
    i, j, k = ak.unzip(ak.combinations(vec, 3))
    best = ak.argmin(abs((i + j + k).mass - 125), axis=1, keepdims=True)
    order_check = ak.concatenate([i[best].mass, j[best].mass, k[best].mass], axis=1)
    largest = ak.argmax(order_check, axis=1, keepdims=True)
    smallest = ak.argmin(order_check, axis=1, keepdims=True)
    leading_particles = ak.concatenate([i[best], j[best], k[best]], axis=1)
    leg1 = leading_particles[largest]
    leg3 = leading_particles[smallest]
    leg2 = leading_particles[
        (leading_particles.idx != ak.flatten(leg1.idx))
        & (leading_particles.idx != ak.flatten(leg3.idx))
    ]
    leg1 = ak.firsts(leg1)
    leg2 = ak.firsts(leg2)
    leg3 = ak.firsts(leg3)
    a12 = np.arccos(leg1.dot(leg2) / (leg1.norm3 * leg2.norm3))
    a13 = np.arccos(leg1.dot(leg3) / (leg1.norm3 * leg3.norm3))
    a23 = np.arccos(leg2.dot(leg3) / (leg2.norm3 * leg3.norm3))
    if not variant:
        color_ring = (a13**2 + a23**2) / (a12**2)
    else:
        color_ring = a13**2 + a23**2 - a12**2
    return color_ring


def d2_calc(fatjet):
    jetdef = fastjet.JetDefinition(
        fastjet.cambridge_algorithm, 0.8
    )  # make this C/A at 0.8
    pf = ak.flatten(fatjet.constituents.pf, axis=1)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    d2 = softdrop_cluster.exclusive_jets_energy_correlator(func="D2")
    return d2


def n4_calc(fatjet):
    jetdef = fastjet.JetDefinition(
        fastjet.cambridge_algorithm, 0.8
    )  # make this C/A at 0.8
    pf = ak.flatten(fatjet.constituents.pf, axis=1)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    e25 = softdrop_cluster.exclusive_jets_energy_correlator(
        func="generalized", angles=2, npoint=5
    )
    e14 = softdrop_cluster.exclusive_jets_energy_correlator(
        func="generalized", angles=1, npoint=4
    )
    n4 = e25 / (e14**2)
    return n4


def d3_calc(fatjet):
    jetdef = fastjet.JetDefinition(
        fastjet.cambridge_algorithm, 0.8
    )  # make this C/A at 0.8
    pf = ak.flatten(fatjet.constituents.pf, axis=1)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    e4 = softdrop_cluster.exclusive_jets_energy_correlator(
        func="generic", normalized=True, npoint=4
    )
    e2 = softdrop_cluster.exclusive_jets_energy_correlator(
        func="generic", normalized=True, npoint=2
    )
    e3 = softdrop_cluster.exclusive_jets_energy_correlator(
        func="generic", normalized=True, npoint=3
    )
    d3 = (e4 * (e2**3)) / (e3**3)
    return d3


def u_calc(fatjet, n):
    jetdef = fastjet.JetDefinition(
        fastjet.cambridge_algorithm, 0.8
    )  # make this C/A at 0.8
    pf = ak.flatten(fatjet.constituents.pf, axis=1)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    if n == 1:
        u = softdrop_cluster.exclusive_jets_energy_correlator(func="u1")
    if n == 2:
        u = softdrop_cluster.exclusive_jets_energy_correlator(func="u2")
    if n == 3:
        u = softdrop_cluster.exclusive_jets_energy_correlator(func="u3")
    return u
