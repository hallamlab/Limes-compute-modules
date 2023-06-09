import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

ASM         = Item('metagenomic assembly')

ORFS        = Item('metagenomic orfs')
GBK         = Item('metagenomic orfs gbk')
SCORES      = Item('metagenomic orf scores')

CONTAINER   = 'prodigal.sif'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    WS = context.output_folder
    REF = P.reference_folder

    asm = M[ASM]
    assert isinstance(asm, Path), f"expected path for asm, got [{asm}]"

    asm_dir = os.path.join(*asm.parts[:-1])
    asm_file = asm.name
    asm_name = ".".join(asm.name.split(".")[:-1])
    asm_name = asm_name[:-4] if asm_name.endswith(".asm") else asm_name

    aa_orfs, scores, gbk_orfs = [f"{asm_name}.{ext}" for ext in [
        "orfs.faa", "orfs.scores", "orfs.gbk"
    ]]
    binds = [
        f"{asm_dir}:/in",
        f"{WS}:/ws",
    ]
    context.shell(f"""\
        singularity run -B {",".join(binds)} {REF.joinpath(CONTAINER)} \
            prodigal -p meta \
            -i /in/{asm_file} -a /ws/{aa_orfs} \
            -o /ws/{gbk_orfs} -s /ws/{scores}
    """)

    return JobResult(
        manifest = {
            ORFS: WS.joinpath(aa_orfs),
            GBK: WS.joinpath(gbk_orfs),
            SCORES: WS.joinpath(scores),
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ASM, groupby=None)\
    .PromiseOutput(ORFS)\
    .PromiseOutput(GBK)\
    .PromiseOutput(SCORES)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=1, memory_gb=4)\
    .SetHome(__file__, name=None)\
    .Build()
