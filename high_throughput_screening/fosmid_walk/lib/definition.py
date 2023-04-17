import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

BACKBONE    = Item("fosmid vector backbone sequence")
DEFAULT_BACKBONE = "pcc1"
READS       = Item("fosmid reads")

FW_FULL     = Item("fosmid pool reads with ends")
FW_HITS     = Item("fosmid pool ends")
FW_STATS    = Item("foswalk stats")

CONTAINER   = "foswalk.sif"

def procedure(context: JobContext) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder
    container = ref.joinpath(CONTAINER)
    TEMP_PREFIX = "TEMP"

    bb = manifest[BACKBONE]
    if isinstance(bb, str) and bb.lower() == DEFAULT_BACKBONE:
        bb = None
    else:
        assert isinstance(bb, Path), f"""expected path or "{DEFAULT_BACKBONE}" for backbone, got [{bb}]"""

    reads = manifest[READS]
    assert isinstance(reads, Path), f"expected path for reads, got [{reads}]"
    
    ws = context.output_folder.joinpath(f"{TEMP_PREFIX}.ws")
    context.shell(f"""\
        mkdir -p {ws}
        cp {reads} {ws}
        {"" if bb is None else f"cp {reads} {ws}"}
    """)

    binds = [
        f"{context.output_folder}:/output",
        f"{ws}:/ws"
    ]

    bb_param = "" if bb is None else f"-b /ws/{bb.name}"
    code = context.shell(f"""\
        PYTHONPATH=""
        singularity run -B {",".join(binds)} {container} \
            foswalk -t {params.threads} \
            -o /output -r /ws/{reads.name} {bb_param} \
    """)

    if code == 0:
        context.shell(f"""\
        rm -rf {context.output_folder.joinpath(TEMP_PREFIX)}*
        """)

    sample_name = ".".join(reads.name.split(".")[:-1])
    return JobResult(
        manifest = {
            FW_FULL: context.output_folder.joinpath(f"{sample_name}_original.fasta"),
            FW_HITS: context.output_folder.joinpath(f"{sample_name}_hits.fasta"),
            FW_STATS: context.output_folder.joinpath(f"{sample_name}.json"),
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(BACKBONE)\
    .AddInput(READS)\
    .PromiseOutput(FW_FULL)\
    .PromiseOutput(FW_HITS)\
    .PromiseOutput(FW_STATS)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=1, memory_gb=8)\
    .SetHome(__file__)\
    .Build()
