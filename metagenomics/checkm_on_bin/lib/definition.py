import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

BIN         = Item('metagenomic bin')

CHECKM_WS   = Item('checkm work')
CHECKM_STATS= Item('checkm stats')

CONTAINER   = 'checkm.sif'
CHECKM_DB   = 'checkm_data_2015_01_16'

# about 10mins on bacterial genome, 1thread / 48ram
def _procedure(context: JobContext) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder
    container = ref.joinpath(CONTAINER)

    binds = [
        f"{ref}/{CHECKM_DB}:/checkm_database",
        f"./:/ws",
    ]

    bin = manifest[BIN]
    assert not isinstance(bin, list), f"expected one bin, but got: {bin}"
    ext = str(bin).split('.')[-1]
    
    in_folder = context.output_folder.joinpath('in')
    tmp_folder = context.output_folder.joinpath('tmp')
    out_folder = context.output_folder.joinpath(f'checkm_ws')
    out_stats = context.output_folder.joinpath('stats.tsv')

    # https://github.com/Ecogenomics/CheckM/wiki/Genome-Quality-Commands
    code = context.shell(f"""\
        mkdir -p {in_folder} {tmp_folder}
        cp -L {bin} {in_folder}
        singularity run -B {",".join(binds)} {container} \
        checkm lineage_wf -x {ext} -t {params.threads} \
            --tmpdir /ws/{tmp_folder} \
            /ws/{in_folder} /ws/{out_folder}
        rm -r {in_folder} {tmp_folder}
        singularity run -B {",".join(binds)} {container} \
            checkm qa --out_format 2 --tab_table \
            /ws/{out_folder}/lineage.ms /ws/{out_folder} > {out_stats}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            CHECKM_WS: out_folder,
            CHECKM_STATS: out_stats,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(_procedure)\
    .AddInput(BIN, groupby=None)\
    .PromiseOutput(CHECKM_WS)\
    .PromiseOutput(CHECKM_STATS)\
    .SuggestedResources(threads=1, memory_gb=48)\
    .Requires({CONTAINER, CHECKM_DB})\
    .SetHome(__file__, name=None)\
    .Build()
