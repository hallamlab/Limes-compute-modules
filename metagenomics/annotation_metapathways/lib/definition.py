from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

BIN         = Item('metagenomic bin')

MP3_WS      = Item('metapathways work')
# ANNOTATIONS = Item('genomic annotations')

CONTAINER   = 'metapathways.sif'
MP3_DB      = 'metapathways_db'
MP3_PARAMS  = 'metapathways_params.txt'

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
    fname = '.'.join(str(bin_path).split('/')[-1].split('.')[:-1])
    out_name = f"{fname}_mp3"
    out_folder = context.output_folder.joinpath(out_name)

    code = context.shell(f"""\
        singularity exec -B {",".join(binds)} {CONTAINER} \
        MetaPathways -p /ref/{MP3_PARAMS} -d /ref/{MP3_DB} -t {params.threads} -v \
            -i /ws/{bin_path}/ \
            -o /ws/{out_folder}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            MP3_WS: out_folder
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(example_procedure)\
    .AddInput(BIN)\
    .PromiseOutput(MP3_WS)\
    .Requires({CONTAINER, MP3_DB, MP3_PARAMS})\
    .SetHome(__file__, name=None)\
    .Build()
