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

### Installing from Source

### From PyPI
```bash
pip install dynamic_data_reduction
```

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

## Usage

- General use example: [examples/simple/simple-example.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/simple/simple-example.py)
- Using Coffea Processors Classes Directly: [examples/coffea_processor/example_with_preprocess.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/coffea_processor/example_with_preprocess.py)
- Coffea use in analysis: [examples/cortado/ddr_cortado.py](https://github.com/cooperative-computing-lab/dynamic_data_reduction/blob/main/examples/cortado/ddr_cortado.py)


## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.