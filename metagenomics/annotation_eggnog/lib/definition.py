from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

from diamond import RunDiamond
import parse_annotations

ORFS        = Item('metagenomic orfs')

HITS        = Item('hits to eggnog')
ANNOTATIONS = Item('annotation by eggnog')

CONTAINER   = "diamond.sif"
PY          = "python_for_data_science.sif"
DB          = 'eggnog_proteins.dmnd'
KB          = 'eggnog.db'

# ~30 mins to run
def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    WS = context.output_folder
    REF = P.reference_folder

    orfs = M[ORFS]
    assert isinstance(orfs, Path), f"expected path for orfs, got [{orfs}]"

    # hits file is overwritten by parser below
    # adds proper header and converts to csv
    run_result, key, hits, columns = RunDiamond(orfs, REF.joinpath(DB), WS, REF.joinpath(CONTAINER), context.shell, P.threads, P.mem_gb)

    py_sif = REF.joinpath(PY)
    parser = Path(parse_annotations.__file__)
    parser_dir = parser.parent
    parser_file = parser.name
    binds = [
        f"{WS}:/ws",
        f"{parser_dir}:/parser_lib",
    ]

    annot_out = f"{key}.annots.csv"
    group_out = f"{key}.groups.csv"
    context.shell(f"""\
        export PYTHONPATH=""
        singularity run -B {",".join(binds)} {py_sif} python /parser_lib/{parser_file} \
            /ws/{hits.name} "{';'.join(columns)}" {REF.joinpath(KB)} /ws/{annot_out} /ws/{group_out}
    """)

    return JobResult(
        manifest = {
            HITS: hits,
            ANNOTATIONS: [WS.joinpath(p) for p in [annot_out, group_out]],
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ORFS, groupby=None)\
    .PromiseOutput(ANNOTATIONS)\
    .PromiseOutput(HITS)\
    .Requires({CONTAINER, PY, DB, KB})\
    .SuggestedResources(threads=8, memory_gb=8)\
    .SetHome(__file__, name=None)\
    .Build()
