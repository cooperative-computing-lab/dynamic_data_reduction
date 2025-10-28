#!/bin/bash
# Integration test that:
# 1. Creates a conda environment from environment.yml
# 2. Installs the package
# 3. Runs the simple example

set -e  # Exit on any error

# Initialize conda for bash
eval "$(conda shell.bash hook)" 2>/dev/null || {
    echo "Error: Could not initialize conda. Make sure conda is installed and in your PATH."
    exit 1
}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Test environment name (use a unique name to avoid conflicts)
TEST_ENV_NAME="ddr-test-$$"
CLEANUP_ENV=true

# Installation source (set INSTALL_FROM_PYPI=1 to install from PyPI instead of local source)
INSTALL_FROM_PYPI=${INSTALL_FROM_PYPI:-0}

echo -e "${YELLOW}=== Dynamic Data Reduction - Environment & Example Test ===${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Test environment name: $TEST_ENV_NAME"
echo "Installation source: $([ "$INSTALL_FROM_PYPI" = "1" ] && echo "PyPI" || echo "Local source")"
echo ""

# Function to cleanup on exit
cleanup() {
    if [ "$CLEANUP_ENV" = true ] && conda env list | grep -q "^${TEST_ENV_NAME} "; then
        echo -e "${YELLOW}Cleaning up test environment: $TEST_ENV_NAME${NC}"
        conda env remove -n "$TEST_ENV_NAME" -y > /dev/null 2>&1 || true
    fi
}

# Register cleanup function to run on exit
trap cleanup EXIT

# Step 1: Create conda environment from environment.yml
echo -e "${YELLOW}Step 1: Creating conda environment from environment.yml${NC}"
cd "$PROJECT_ROOT"

# Create the conda environment with a custom name
if conda env create -f environment.yml -n "$TEST_ENV_NAME" -y; then
    echo -e "${GREEN}✓ Conda environment created successfully${NC}"
else
    echo -e "${RED}✗ Failed to create conda environment${NC}"
    exit 1
fi

# Step 2: Verify Python version
echo -e "${YELLOW}Step 2: Verifying Python installation${NC}"
PYTHON_VERSION=$(conda run -n "$TEST_ENV_NAME" python --version)
echo "Python version: $PYTHON_VERSION"
echo -e "${GREEN}✓ Python version verified${NC}"

# Step 3: Verify key packages are installed
echo -e "${YELLOW}Step 3: Verifying key packages are installed${NC}"
PACKAGES_TO_CHECK=("coffea" "ndcctools" "uproot" "rich")
for package in "${PACKAGES_TO_CHECK[@]}"; do
    if conda run -n "$TEST_ENV_NAME" conda list | grep -q "$package"; then
        echo -e "  ${GREEN}✓${NC} $package is installed"
    else
        echo -e "  ${RED}✗${NC} $package is NOT installed"
        exit 1
    fi
done

# Step 4: Install the package
echo -e "${YELLOW}Step 4: Installing dynamic_data_reduction package${NC}"
if [ "$INSTALL_FROM_PYPI" = "1" ]; then
    echo "Installing from PyPI..."
    if conda run -n "$TEST_ENV_NAME" pip install dynamic_data_reduction; then
        echo -e "${GREEN}✓ Package installed successfully from PyPI${NC}"
    else
        echo -e "${RED}✗ Failed to install package from PyPI${NC}"
        exit 1
    fi
else
    echo "Installing from local source..."
    if conda run -n "$TEST_ENV_NAME" pip install -e "$PROJECT_ROOT"; then
        echo -e "${GREEN}✓ Package installed successfully from local source${NC}"
    else
        echo -e "${RED}✗ Failed to install package from local source${NC}"
        exit 1
    fi
fi

# Step 5: Run the simple example
echo -e "${YELLOW}Step 5: Running simple example${NC}"
EXAMPLE_FILE="$PROJECT_ROOT/examples/simple/simple-example.py"

if [ ! -f "$EXAMPLE_FILE" ]; then
    echo -e "${RED}✗ Example file not found: $EXAMPLE_FILE${NC}"
    exit 1
fi

echo "Running: $EXAMPLE_FILE"
echo "---"

# Run the example (output shown in real-time)
if conda run -n "$TEST_ENV_NAME" python "$EXAMPLE_FILE"; then
    echo "---"
    echo -e "${GREEN}✓ Simple example ran successfully${NC}"
else
    echo "---"
    echo -e "${RED}✗ Simple example failed${NC}"
    exit 1
fi

# All tests passed
echo ""
echo -e "${GREEN}=== All tests passed! ===${NC}"
echo ""
echo "Summary:"
echo "  ✓ Conda environment created from environment.yml"
echo "  ✓ Python and required packages verified"
echo "  ✓ Package installed successfully"
echo "  ✓ Simple example executed successfully"
exit 0

