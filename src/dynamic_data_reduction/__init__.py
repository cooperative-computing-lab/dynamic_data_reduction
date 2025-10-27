"""
Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.
"""

__version__ = "0.1.0"

# Import main classes/functions to make them available at package level
from .main import DynamicDataReduction, ProcT, ResultT
from .ddr_coffea import CoffeaDynamicDataReduction
from .coffea_dataset_tools import preprocess

__all__ = [
    DynamicDataReduction,
    CoffeaDynamicDataReduction,
    ProcT,
    ResultT,
    preprocess,
]
