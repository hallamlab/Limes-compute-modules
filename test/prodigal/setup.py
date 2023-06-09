import os, sys
sys.path = list(set([
    "../../../Limes-x/src/"
]+sys.path))
from pathlib import Path
import limes_x as lx
from limes_x import ComputeModule


ref_dir = Path("../../../lx_ref")
os.makedirs(ref_dir, exist_ok=True)

HERE = Path("../../")

modules = []
# modules += ComputeModule.LoadSet(HERE.joinpath("logistics"))
modules += ComputeModule.LoadSet(HERE.joinpath("metagenomics"))
modules = [m for m in modules if m.name == "orf_prediction"]
# modules += ComputeModule.LoadSet(HERE.joinpath("high_throughput_screening"))
wf = lx.Workflow(modules, ref_dir)
wf.Setup('singularity')
