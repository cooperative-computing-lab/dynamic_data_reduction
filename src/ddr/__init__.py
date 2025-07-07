"""
Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.
"""

__version__ = "0.1.0"

# Import main classes/functions to make them available at package level
from .ddr import *  # Import main functionality
from .ddr_coffea import *  # Import Coffea specialization

__all__ = [
    DynamicDataReduction,
    CoffeaDynamicDataReduction
]
