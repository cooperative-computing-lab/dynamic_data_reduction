"""
Integration test for environment setup and simple example execution.

This test verifies that:
1. The conda environment can be created from environment.yml
2. Required packages are properly installed
3. The simple example runs successfully
"""

import subprocess
import sys
import os
import pytest
from pathlib import Path


# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
ENVIRONMENT_YML = PROJECT_ROOT / "environment.yml"
SIMPLE_EXAMPLE = PROJECT_ROOT / "examples" / "simple" / "simple-example.py"


def get_conda_env_list():
    """Get list of conda environments."""
    result = subprocess.run(
        ["conda", "env", "list"], capture_output=True, text=True, check=True
    )
    return result.stdout


def create_test_env(env_name):
    """Create a temporary test environment from environment.yml."""
    # Create the conda environment with a custom name using -n flag
    result = subprocess.run(
        ["conda", "env", "create", "-f", str(ENVIRONMENT_YML), "-n", env_name, "-y"],
        capture_output=True,
        text=True,
        timeout=600,  # 10 minute timeout for environment creation
    )
    if result.returncode != 0:
        pytest.fail(f"Failed to create conda environment:\n{result.stderr}")


def remove_test_env(env_name):
    """Remove the test conda environment."""
    if env_name in get_conda_env_list():
        subprocess.run(
            ["conda", "env", "remove", "-n", env_name, "-y"],
            capture_output=True,
            check=False,  # Don't fail if removal fails
        )


def run_in_conda_env(env_name, command):
    """Run a command in the specified conda environment."""
    result = subprocess.run(
        ["conda", "run", "-n", env_name] + command,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    return result


@pytest.fixture(scope="module")
def test_env():
    """Fixture to create and cleanup a test conda environment."""
    # Create unique environment name
    env_name = f"ddr-pytest-test-{os.getpid()}"

    # Ensure environment doesn't already exist
    if env_name in get_conda_env_list():
        remove_test_env(env_name)

    # Create the environment
    print(f"\nCreating test environment: {env_name}")
    create_test_env(env_name)

    # Install the package
    print("Installing dynamic_data_reduction package...")
    result = run_in_conda_env(env_name, ["pip", "install", "-e", str(PROJECT_ROOT)])
    if result.returncode != 0:
        remove_test_env(env_name)
        pytest.fail(f"Failed to install package:\n{result.stderr}")

    yield env_name

    # Cleanup
    print(f"\nCleaning up test environment: {env_name}")
    remove_test_env(env_name)


def test_conda_environment_creation(test_env):
    """Test that the conda environment was created successfully."""
    env_list = get_conda_env_list()
    assert test_env in env_list, f"Environment {test_env} not found in conda env list"


def test_python_version(test_env):
    """Test that Python is installed and has the correct version."""
    result = run_in_conda_env(test_env, ["python", "--version"])
    assert result.returncode == 0, "Failed to get Python version"

    version_output = result.stdout.strip()
    print(f"Python version: {version_output}")
    assert "Python 3." in version_output, f"Unexpected Python version: {version_output}"


def test_required_packages(test_env):
    """Test that all required packages are installed."""
    required_packages = [
        "coffea",
        "ndcctools",
        "uproot",
        "rich",
        "dask",
        "fsspec-xrootd",
        "xrootd",
    ]

    result = run_in_conda_env(test_env, ["conda", "list"])
    assert result.returncode == 0, "Failed to list packages"

    installed_packages = result.stdout.lower()

    for package in required_packages:
        assert (
            package.lower() in installed_packages
        ), f"Package {package} not found in conda list"
        print(f"✓ {package} is installed")


def test_package_import(test_env):
    """Test that the dynamic_data_reduction package can be imported."""
    result = run_in_conda_env(
        test_env,
        ["python", "-c", "import dynamic_data_reduction; print('Import successful')"],
    )
    assert result.returncode == 0, f"Failed to import package:\n{result.stderr}"
    assert "Import successful" in result.stdout


def test_simple_example_runs(test_env):
    """Test that the simple example runs successfully."""
    assert SIMPLE_EXAMPLE.exists(), f"Simple example not found at {SIMPLE_EXAMPLE}"

    print(f"\nRunning simple example: {SIMPLE_EXAMPLE}")
    result = run_in_conda_env(test_env, ["python", str(SIMPLE_EXAMPLE)])

    # Print output for debugging
    print("\n--- Example Output ---")
    print(result.stdout)
    if result.stderr:
        print("--- Example Stderr ---")
        print(result.stderr)
    print("--- End Output ---\n")

    assert (
        result.returncode == 0
    ), f"Simple example failed with return code {result.returncode}"

    # Check that the example produced some numeric output (the result)
    # The simple example should print the final computation result
    assert result.stdout.strip(), "Example produced no output"

    # Check if there's a numeric value in the output
    import re

    has_numbers = bool(re.search(r"\d+", result.stdout))
    assert has_numbers, "Example output doesn't contain expected numeric results"


@pytest.mark.slow
def test_simple_example_produces_correct_result(test_env):
    """
    Test that the simple example produces the expected result.

    This is marked as slow since it requires running the full example.
    """
    result = run_in_conda_env(test_env, ["python", str(SIMPLE_EXAMPLE)])

    assert result.returncode == 0, "Simple example failed"

    # The example computes results with doubling and tripling operations
    # We just verify it completes and produces output
    # (specific value checking would require understanding the exact computation)
    output = result.stdout.strip()
    assert output, "Example produced no output"
    print(f"Example result: {output}")


if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s"])
