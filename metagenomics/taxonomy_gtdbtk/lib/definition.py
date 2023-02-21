import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

BIN         = Item('metagenomic bin')

GTDBTK_WS   = Item('gtdbtk work')
GTDBTK_TAX  = Item('gtdbtk taxonomy table')

CONTAINER   = 'gtdbtk.sif'
GTDBTK_DB   = 'gtdbtk_data'

def example_procedure(context: JobContext) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder

    TEMP_PREFIX = "temp"
    temp_dir = context.output_folder.joinpath(f"{TEMP_PREFIX}.ws")
    container = ref.joinpath(CONTAINER)

    bin_path = manifest[BIN]
    assert isinstance(bin_path, Path), f"expected only one bin {bin_path}"
    bin_folder = context.output_folder.joinpath(f"{TEMP_PREFIX}.bin_input")
    name = f"{context.job_id}-{'.'.join(bin_path.name.split('.')[:-1])}"
    out_folder = context.output_folder.joinpath(f"{name}_gtdbtk")

    binds = [
        f"{ref.joinpath(GTDBTK_DB)}:/ref",
        f"{temp_dir}:/gtdbtk_temp",
        f"./:/ws",
    ]

    code = context.shell(f"""\
        echo "bin path: {bin_path}"
        mkdir -p {bin_folder}
        mkdir -p {temp_dir}
        cp {bin_path} {bin_folder.joinpath(bin_path.name)}
        singularity run -B {",".join(binds)} {container} \
        gtdbtk classify_wf -x fa --cpus {params.threads} --pplacer_cpus {int(min(params.threads, params.mem_gb//(40+1)))} \
            --tmpdir /gtdbtk_temp \
            --genome_dir /ws/{bin_folder} \
            --out_dir /ws/{out_folder}
    """)
        # cd {context.output_folder} && rm -r {TEMP_PREFIX}*

    classify_out = out_folder.joinpath("classify")
    file_candidates = os.listdir(classify_out) if classify_out.exists() else []
    file_candidates = [classify_out.joinpath(f) for f in file_candidates if f.endswith("summary.tsv")]
    if len(file_candidates) != 1:
        return JobResult(
            exit_code = 1,
            error_message = f"didn't finish; no summary table produced: {file_candidates}",
                    manifest = {
            GTDBTK_WS: out_folder,
            },
        )
    summary = context.output_folder.joinpath(f"{name}_gtdbtk-tax.tsv")

    context.shell(f"""\
        cp {file_candidates[0]} {summary}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            GTDBTK_WS: out_folder,
            GTDBTK_TAX: summary,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(example_procedure)\
    .AddInput(BIN, groupby=None)\
    .PromiseOutput(GTDBTK_WS)\
    .PromiseOutput(GTDBTK_TAX)\
    .SuggestedResources(threads=1, memory_gb=48)\
    .Requires({GTDBTK_DB})\
    .SetHome(__file__, name=None)\
    .Build()
