# DDR - Dynamic MapReduce Framework

A flexible framework for distributed data processing using MapReduce patterns.

## Installation

### From PyPI (when published)
```bash
pip install ddr
```

### From source
```bash
git clone https://github.com/yourusername/ddr.git
cd ddr
pip install -e .
```

## Usage

### Basic usage
```python
from ddr import ddr

# Your code here
```

### Coffea specialization
```python
from ddr import ddr_coffea

# Your Coffea-specific code here
```

### Example usage
-- General use example: simple-example.py
-- Coffea use in analysis: ddr_cortado.py

```python
from ddr import ddr_cortado

# Run the example
```

## Development

To set up the development environment:

```bash
git clone https://github.com/yourusername/ddr.git
cd ddr
pip install -e ".[dev]"
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
