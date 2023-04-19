import os, sys
from pathlib import Path
import limes_x as lx
from limes_x import ComputeModule

assert len(sys.argv) > 1, f"no installation path given"
ref_dir = Path(sys.argv[1])
os.makedirs(ref_dir, exist_ok=True)

SCRIPT = Path(os.path.abspath(__file__))

modules = []
modules += ComputeModule.LoadSet(SCRIPT.joinpath("logistics"))
modules += ComputeModule.LoadSet(SCRIPT.joinpath("metagenomics"))
modules += ComputeModule.LoadSet(SCRIPT.joinpath("high_throughput_screening"))
modules = [m for m in modules if "dram" not in m.name]
wf = lx.Workflow(modules, ref_dir)
wf.Setup('singularity')
