import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sra accession')
BIN         = Item('metagenomic bin')

MP3_WS      = Item('metapathways work')
ANNOTATIONS = Item('genomic annotation')

CONTAINER   = 'metapathways.sif'
MP3_DB      = 'metapathways_db'
MP3_PARAMS  = 'metapathways_params.txt'
PIGZ        = 'pigz'

def example_procedure(context: JobContext) -> JobResult:
    manifest = context.manifest
    params = context.params
    ref = params.reference_folder
    binds = [
        f"{ref}:/ref",
        f"./:/ws",
    ]

    container = ref.joinpath(CONTAINER)
    pigz = ref.joinpath(PIGZ)

    TEMP_PREFIX = "temp"
    input_dir = context.output_folder.joinpath(f"{TEMP_PREFIX}.inputs")
    context.shell(f"mkdir -p {input_dir}")

    IN_PREFIX = "in-"
    bin_paths = manifest[BIN]
    if not isinstance(bin_paths, list): bin_paths = [bin_paths]
    for p in bin_paths:
        assert isinstance(p, Path), f"expected a path for ea bin, got {p}"
        toks = f"{IN_PREFIX}{p.name}".split('.') # mp3 requires that it start with a letter
        fname = '_'.join(toks[:-1]) # no "." in name
        ext = toks[-1]
        context.shell(f"cp {p} {input_dir}/{fname}.{ext}")

    sample = manifest[SAMPLE]
    assert isinstance(sample, str), f"expected str for sample, got {sample}"
    out_name = f"{sample}_mp3ws".replace(" ", "_")
    out_folder = context.output_folder.joinpath(out_name)

    code = context.shell(f"""\
        singularity run -B {",".join(binds)} {container} \
        MetaPathways -p /ref/{MP3_PARAMS} -d /ref/{MP3_DB} -t {params.threads} -v \
            -i /ws/{input_dir}/ \
            -o /ws/{out_folder}
        cd {context.output_folder} && rm -r {TEMP_PREFIX}*
    """)

    annotations = []
    for out_dir in os.listdir(out_folder):
        out_path = f"{out_folder}/{out_dir}"
        if not os.path.isdir(out_path): continue

        result = out_dir.replace(IN_PREFIX, "")+"_mp3"
        result_zip = f"{result}.tar.gz"
        annotations.append(context.output_folder.joinpath(result_zip))
        _code = context.shell(f"""\
            cd {context.output_folder}
            mkdir -p {result}
            cp -r {out_name}/{out_dir}/orf_prediction/* {result}/
            cp -r {out_name}/{out_dir}/results/* {result}/
            cp {out_name}/{out_dir}/ptools/0.pf {result}/ptools_input.pf
            tar -cf - {result} | {pigz} -7 -p {params.threads} >{result_zip} && rm -r {result}
        """)
        code = max(1, _code+code)

    out_folder_zip = Path(f"{out_folder}.tar.gz")
    context.shell(f"""\
        cd {context.output_folder}
        tar -cf - {out_folder} | {pigz} -7 -p {params.threads} >{out_folder_zip} && rm -r {out_folder}
    """)

    return JobResult(
        manifest = {
            MP3_WS: out_folder_zip,
            ANNOTATIONS: annotations,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(example_procedure)\
    .AddInput(SAMPLE)\
    .AddInput(BIN, groupby=SAMPLE)\
    .PromiseOutput(MP3_WS)\
    .PromiseOutput(ANNOTATIONS)\
    .Requires({CONTAINER, PIGZ, MP3_DB, MP3_PARAMS})\
    .SuggestedResources(threads=4, memory_gb=48)\
    .SetHome(__file__, name=None)\
    .Build()
