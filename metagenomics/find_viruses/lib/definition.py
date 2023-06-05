import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

ASM         = Item('metagenomic assembly')

V_CONTIGS   = Item('metagenomic viral contigs')
V_SCORES    = Item('metagenomic viral contig scores')
V_LOCS      = Item('metagenomic viral contig locations')
V_WORK        = Item('metagenomic find virus work')

CONTAINER   = 'virsorter.sif'
REF_DB      = 'virsorter_db'
REF_SRC     = 'virsorter_src'
# REF_DB      = 'virsorter_db.tgz'
PIGZ        = 'pigz'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    WS = context.output_folder
    REF = P.reference_folder
    TEMP = "TEMP"
    pigz = REF.joinpath(PIGZ)

    asm = M[ASM]
    assert isinstance(asm, Path), f"expected path for asm, got [{asm}]"

    # ref_db = REF.joinpath(REF_DB)
    # if f"{ref_db}".endswith("gz"):
    #     ref_redirect = WS.joinpath(f"{TEMP}_db")
    #     context.shell(f"""\
    #         {pigz} -dc {ref_db} | tar -xf - -C {ref_redirect}
    #     """)
    # else:
    #     ref_redirect = REF
    ref_redirect = REF

    asm_dir = os.path.join(*asm.parts[:-1])
    asm_file = asm.name
    asm_name = ".".join(asm.name.split(".")[:-1])

    contigs, scores, locs, work = [f"{asm_name}.vir.{ext}" for ext in [
        "fa", "scores.tsv", "bounds.tsv", "work.tgz"
    ]]
    binds = [
        f"{ref_redirect}:/ref",
        f"{REF.joinpath(REF_SRC)}:/VirSorter2/virsorter",
        f"{asm_dir}:/in",
        f"{WS}:/ws",
    ]
    context.shell(f"""\
        PYTHONPATH=""
        singularity run -B {",".join(binds)} {REF.joinpath(CONTAINER)} \
            virsorter run --min-length 1000 -j {P.threads} all \
            --db-dir /ref/virsorter_db \
            -i /in/{asm_file} \
            -w /ws/{TEMP}

        cd {WS}
        mv {TEMP}/final-viral-combined.fa {contigs}
        mv {TEMP}/final-viral-score.tsv {scores}
        mv {TEMP}/final-viral-boundary.tsv {locs}
        tar -cf - {TEMP} | {pigz} -7 -p {P.threads} >{work}
    """)

    return JobResult(
        manifest = {
            V_CONTIGS: WS.joinpath(contigs),
            V_SCORES: WS.joinpath(scores),
            V_LOCS: WS.joinpath(locs),
            V_WORK: WS.joinpath(work),
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ASM, groupby=None)\
    .PromiseOutput(V_CONTIGS)\
    .PromiseOutput(V_SCORES)\
    .PromiseOutput(V_LOCS)\
    .PromiseOutput(V_WORK)\
    .Requires({CONTAINER, REF_DB, REF_SRC, PIGZ})\
    .SuggestedResources(threads=4, memory_gb=8)\
    .SetHome(__file__, name=None)\
    .Build()
