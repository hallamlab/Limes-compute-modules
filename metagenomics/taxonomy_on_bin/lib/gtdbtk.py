import os
from pathlib import Path
from limes_x import JobContext, JobResult

def gtdbtk_procedure(context: JobContext, SAMPLE, BINS, GTDBTK_WS, GTDBTK_TAX, CONTAINER, GTDBTK_DB) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder

    TEMP_PREFIX = "temp"
    temp_dir = context.output_folder.joinpath(f"{TEMP_PREFIX}.ws")
    container = ref.joinpath(CONTAINER)

    sample = manifest[SAMPLE]
    assert isinstance(sample, str), f"expected str for sample, got {sample}"

    bin_paths = manifest[BINS]
    if not isinstance(bin_paths, list): bin_paths = [bin_paths]
    bin_folder = context.output_folder.joinpath(f"{TEMP_PREFIX}.bin_input")
    out_folder = context.output_folder.joinpath(f"{sample}_gtdbtk")

    context.shell(f"mkdir -p {bin_folder}")
    seen_names = {}
    ext = "fa"
    for p in bin_paths:
        assert isinstance(p, Path), f"expected path, but got: {p}"
        toks = str(p.name).split('.')
        fname = ".".join(toks[:-1])
        if fname in seen_names:
            i = seen_names[fname]+1
            seen_names[fname] = i
            fname = f"{fname}_{i}"
        else:
            seen_names[fname] = 1

        name = f"{sample}-{fname}.{ext}"
        context.shell(f"""\
            cp {p} {bin_folder.joinpath(name)}
        """)
    context.shell(f"""\
        ls -lh {bin_folder}
    """)

    binds = [
        f"{ref.joinpath(GTDBTK_DB)}:/ref",
        f"{temp_dir}:/gtdbtk_temp",
        f"./:/ws",
    ]

    code = context.shell(f"""\
        mkdir -p {temp_dir}
        singularity run -B {",".join(binds)} {container} \
        gtdbtk classify_wf -x {ext} \
            --cpus {params.threads} --pplacer_cpus {int(min(params.threads, params.mem_gb//(40+1)))} \
            --force --tmpdir /gtdbtk_temp \
            --genome_dir /ws/{bin_folder} \
            --out_dir /ws/{out_folder}
    """)

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

    summary = context.output_folder.joinpath(f"{sample}-bins.tax.tsv")
    context.shell(f"""\
        cp {file_candidates[0]} {summary}
    """)

    #clean up
    context.shell(f"""\
        cd {context.output_folder} && rm -r {TEMP_PREFIX}*
    """)

    return JobResult(
        manifest = {
            GTDBTK_WS: out_folder,
            GTDBTK_TAX: summary,
        },
    )