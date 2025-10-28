"""
Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.
"""

# Check for required ndcctools.taskvine dependency
try:
    import ndcctools.taskvine
except ImportError as e:
    raise ImportError(
        "ndcctools.taskvine is required but not installed. "
        "Please install it via conda:\n"
        "  conda install -c conda-forge ndcctools"
    ) from e

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
