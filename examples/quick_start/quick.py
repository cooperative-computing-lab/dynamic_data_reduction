from dynamic_data_reduction import DynamicDataReduction
import ndcctools.taskvine as vine
import getpass

# Simple data: process two datasets
data = {
    "datasets": {
        "numbers": {"values": [1, 2, 3, 4, 5]},
        "more_numbers": {"values": [10, 20, 30]},
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

ddr = DynamicDataReduction(
    mgr,
    data=data,
    source_preprocess=preprocess,
    source_postprocess=postprocess,
    processors=processor,
    accumulator=reducer,
)

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
