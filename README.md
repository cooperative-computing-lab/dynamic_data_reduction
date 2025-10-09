# DDR - Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.

## Installation

### From PyPI (when published)
```bash
pip install dynamic_data_reduction
```

### From source
```bash
git clone https://github.com/yourusername/dynamic_data_reduction.git
cd dynamic_data_reduction
pip install -e .
```

## Usage

### Basic usage
```python
import dynamic_data_reduction as ddr

# Your code here
```

### Coffea specialization
```python
import dynamic_data_reduction.ddr_coffea as ddr_coffea

# Your Coffea-specific code here
```

### Example usage
-- General use example: simple-example.py
-- Coffea use in analysis: ddr_cortado.py

```python
import dynamic_data_reduction.ddr_cortado as ddr_cortado

# Run the example
```

## Development

To set up the development environment:

```bash
git clone https://github.com/yourusername/dynamic_data_reduction.git
cd dynamic_data_reduction
pip install -e ".[dev]"
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
