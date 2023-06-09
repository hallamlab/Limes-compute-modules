import os, sys
sys.path = list(set([
    "../../lib/"
]+sys.path))
from pathlib import Path
import limes_x as lx

modules = []
# modules += lx.LoadComputeModules("../../../Limes-compute-modules/logistics")
modules += lx.LoadComputeModules("../../metagenomics")

WS = Path("./cache/test_workspace")
os.system(f"rm -r {WS}")
wf = lx.Workflow(modules, reference_folder="../../../lx_ref")
wf.Run(
    workspace=WS,
    targets=[
        lx.Item('annotation by mobileogdb'),
        lx.Item('annotation by eggnog'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "DRR001142"), 
            children={
                lx.Item("metagenomic assembly"): Path("./cache/DRR001142.asm.ss.fa"),
            },
        )
    ],
    executor=lx.Executor(),
    params=lx.Params(
        threads=12, mem_gb=14,
    )
)
