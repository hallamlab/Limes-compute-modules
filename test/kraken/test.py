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
        lx.Item('reads taxonomy table'),
        # lx.Item('metagenomic gzipped reads'),
        # lx.Item('metagenomic assembly'),
        # lx.Item("metagenomic bin"),
        # lx.Item("checkm stats"),
        # lx.Item('assembly taxonomy table'),
        # lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR10140508"), 
            children={
                lx.Item("username"): "tony",
                lx.Item("metagenomic gzipped reads"): [
                    Path(p) for p in [
                        "./cache/SRR10140508_1.fastq.gz",
                        "./cache/SRR10140508_2.fastq.gz",
                    ]
                ]
            },
        )
    ],
    executor=lx.Executor(),
    params=lx.Params(
        mem_gb=14,
    )
)
