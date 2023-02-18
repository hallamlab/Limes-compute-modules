import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

BIN         = Item('metagenomic bin')

GTDBTK_WS   = Item('gtdbtk work')
GTDBTK_TAX  = Item('gtdbtk taxonomy table')
# ANNOTATIONS = Item('genomic annotations')

CONTAINER   = 'gtdbtk.sif'
GTDBTK_DB   = 'gtdbtk_data'

def example_procedure(context: JobContext) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder
    binds = [
        f"{ref}:/ref",
        f"./:/ws",
    ]

    bin_path = manifest[BIN]
    assert isinstance(bin_path, Path), f"expected only one bin {bin_path}"
    toks = str(bin_path).split('/')
    bin_folder = '/'.join(toks[:-1])
    fname = '.'.join(toks[-1].split('.')[:-1])
    out_folder = context.output_folder.joinpath(fname)

    code = context.shell(f"""\
        singularity run -B {",".join(binds)} {CONTAINER} \
        gtdbtk classify_wf -x fa --cpus {params.threads} --pplacer_cpus {1} \
            --tmpdir /ws/tmp \
            --genome_dir /ws/{bin_folder} \
            --out_dir /ws/{out_folder}
    """)

    classify_out = context.output_folder.joinpath("classify")
    file_candidates = os.listdir(classify_out) if classify_out.exists() else []
    file_candidates = [classify_out.joinpath(f) for f in file_candidates if f.endswith("summary.tsv")]
    if len(file_candidates) == 0:
        return JobResult(
            exit_code = 1,
            error_message = "didn't finish; no summary table produced",
                    manifest = {
            GTDBTK_WS: out_folder,
            },
        )

    return JobResult(
        exit_code = code,
        manifest = {
            GTDBTK_WS: out_folder,
            GTDBTK_TAX: file_candidates[0],
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(example_procedure)\
    .AddInput(BIN, groupby=None)\
    .PromiseOutput(GTDBTK_WS)\
    .PromiseOutput(GTDBTK_TAX)\
    .SuggestedResources(threads=1, memory_gb=64)\
    .Requires({GTDBTK_DB})\
    .SetHome(__file__, name=None)\
    .Build()
