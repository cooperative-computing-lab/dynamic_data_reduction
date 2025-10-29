# DDR - Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.

## Installation

### Prerequisites

This project requires Python 3.13+ and uses conda for dependency management. We recommend using the provided `environment.yml` file to create a consistent development environment.

### Setting up the Conda Environment

The project includes an `environment.yml` file with the following dependencies:

```yaml
name: ddr
channels:
  - conda-forge
dependencies:
  - coffea=>2025.3.0
  - fsspec-xrootd=>0.5.1
  - ndcctools>=7.15.8
  - python=>3.12
  - rich=>13.9.4
  - uproot=>5.6.0
  - xrootd=>5.8.1
  - setuptools<81
```

1. **Create the conda environment from the provided environment.yml file:**
   ```bash
   conda env create -f environment.yml
   ```

2. **Activate the environment:**
   ```bash
   conda activate ddr
   ```

3. **Verify the installation:**
   ```bash
   python --version  # Should show Python 3.13.2
   conda list | grep -E "(coffea|ndcctools)"  # Should show the installed packages
   ```

### From PyPI
```bash
pip install dynamic_data_reduction
```

### Installing from Source

Once you have the conda environment set up:

```bash
# Clone the repository
git clone https://github.com/cooperative-computing-lab/dynamic_data_reduction.git
cd dynamic_data_reduction

# Activate the conda environment (if not already active)
conda activate ddr

# Install the package in development mode
pip install -e .
```


## Quick Start

Minimal toy example to get started:

```python
from dynamic_data_reduction import DynamicDataReduction
import ndcctools.taskvine as vine
import getpass

# Simple data: process two datasets
data = {
    "datasets": {
        "numbers": {"values": [1, 2, 3, 4, 5]},
        "more_numbers": {"values": [10, 20, 30]}
    }
}

# Define functions
def preprocess(dataset_info, **kwargs):
    for val in dataset_info["values"]:
        yield (val, 1)

def postprocess(val, **kwargs):
    return val  # Just return the value

def processor(x):
    return x * 2  # Double each number

def reducer(a, b):
    return a + b  # Sum the results

# Run
mgr = vine.Manager(port=[9123, 9129], name=f"{getpass.getuser()}-quick-start-ddr")
print(f"Manager started on port {mgr.port}")
ddr = DynamicDataReduction(mgr,
                           data=data,
                           source_preprocess=preprocess, 
                           source_postprocess=postprocess,
                           processors=processor, 
                           accumulator=reducer)

# Use local workers, condor, slurm, or sge for scale
workers = vine.Factory("local", manager=mgr)
workers.max_workers = 2
workers.min_workers = 0
workers.cores = 4
workers.memory = 2000
workers.disk = 8000
with workers:
    result = ddr.compute()

print(f"Result: {result}")  # Expected: (1+2+3+4+5)*2 + (10+20+30)*2 = 150
```

## Usage

- General use example: [examples/simple/simple-example.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/simple/simple-example.py)
- Using Coffea Processors Classes Directly: [examples/coffea_processor/example_with_preprocess.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/coffea_processor/example_with_preprocess.py)
- Coffea use in analysis: [examples/cortado/ddr_cortado.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/cortado/ddr_cortado.py)


## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.