# Integration Tests for Dynamic Data Reduction

This directory contains integration tests that verify the complete setup and execution of the Dynamic Data Reduction framework.

## Test Files

### 1. `test_environment_and_simple_example.sh` (Bash Script)

A comprehensive bash script that tests the full environment setup and example execution.

**What it tests:**
- Creates a conda environment from `environment.yml`
- Verifies Python installation and version
- Checks that all required packages are installed (coffea, ndcctools, uproot, rich)
- Installs the `dynamic_data_reduction` package
- Runs the simple example from `examples/simple/simple-example.py`
- Validates the example produces expected output

**How to run:**
```bash
# From the project root
./tests/test_environment_and_simple_example.sh

# Or from anywhere
bash /path/to/dynamic_data_reduction/tests/test_environment_and_simple_example.sh
```

**Features:**
- Built-in conda initialization (no manual setup required)
- Automatic cleanup of test environment on exit
- Color-coded output for better readability
- Detailed error reporting
- Uses a unique environment name to avoid conflicts with existing environments

### 2. `test_integration_simple_example.py` (Pytest)

A Python-based integration test that can be run with pytest and integrated into CI/CD pipelines.

**What it tests:**
- Conda environment creation from `environment.yml`
- Python version verification
- Required package installation
- Package import capability
- Simple example execution
- Result validation

**How to run:**

```bash
# Run with pytest (from project root, in your existing environment)
conda activate cortado  # or your development environment
pytest tests/test_integration_simple_example.py -v -s

# Run with markers
pytest tests/test_integration_simple_example.py -v -s -m "not slow"  # Skip slow tests

# Run directly as a Python script
python tests/test_integration_simple_example.py
```

**Features:**
- Fixture-based environment management
- Automatic cleanup after tests
- Detailed output capture
- Can be integrated into pytest test suite
- Marked with `@pytest.mark.slow` for long-running tests

## Requirements

Both test options require:
- Conda or Miniconda installed
- Access to conda-forge channel
- Sufficient disk space for environment creation (~2-3 GB)
- Internet connection for package downloads

## Test Environment Isolation

Both tests create temporary, isolated conda environments:
- Bash script: `ddr-test-$$` (where $$ is the process ID)
- Pytest: `ddr-pytest-test-{pid}` (where {pid} is the process ID)

These environments are automatically cleaned up after the tests complete.

## Continuous Integration

For CI/CD pipelines, we recommend using the pytest version:

```yaml
# Example GitHub Actions workflow
- name: Run integration tests
  run: |
    conda activate cortado
    pytest tests/test_integration_simple_example.py -v
```

## Troubleshooting

### Environment creation fails
- Check that you have access to conda-forge channel
- Verify internet connectivity
- Ensure sufficient disk space

### Example execution fails
- Check that all dependencies are properly installed
- Verify that the TaskVine port range (9123-9129) is available
- Check for any firewall restrictions

### Tests hang or timeout
- The environment creation can take 5-10 minutes on first run
- Example execution typically takes 30-60 seconds
- Both tests have built-in timeouts to prevent infinite hangs

## Manual Testing

To manually test the environment setup:

```bash
# Create the environment
conda env create -f environment.yml

# Activate it
conda activate ddr

# Install the package
pip install -e .

# Run the simple example
python examples/simple/simple-example.py
```

## Adding New Tests

When adding new integration tests:
1. Use unique environment names to avoid conflicts
2. Always implement cleanup (try/finally or fixtures)
3. Provide detailed error messages
4. Test both successful execution and error handling
5. Document expected behavior and outputs

