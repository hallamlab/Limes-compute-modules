import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

ACCESSION   = Item('sra accession')
RAW         = Item('sra raw')
USERNAME    = Item('username')

EXTRACTED   = Item('sra extracted')

CONTAINER   = 'sratk.sif'
PIGZ        = 'pigz'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    OUT_DIR = context.output_folder
    TEMP_PREFIX = "temp"
    container = P.reference_folder.joinpath(CONTAINER)
    pigz = P.reference_folder.joinpath(PIGZ)

    fake_home = OUT_DIR.joinpath(f"{TEMP_PREFIX}.home")
    context.shell(f"mkdir -p {fake_home}")

    raw_sra = M[RAW]
    assert isinstance(raw_sra, Path)

    accession = M[ACCESSION]
    assert isinstance(accession, str)
    inputs_dir = OUT_DIR.joinpath(f"{TEMP_PREFIX}.inputs")
    context.shell(f"""\
        mkdir -p {inputs_dir.joinpath(accession)}
        cp -r {raw_sra}/* {inputs_dir.joinpath(accession)}/
    """)

    user = M[USERNAME]
    assert isinstance(user, str)

    binds = [
        f"{fake_home}:/home/{user}",
        f"{inputs_dir}:/inputs",
        f"{OUT_DIR}:/ws",
    ]

    code = context.shell(f"""\
        singularity run -B {",".join(binds)} {container} \
            fasterq-dump --threads {P.threads} --outdir /ws/{accession} /inputs/{accession} 
    """)
    if code != 0:
        code = context.shell(f"""\
            echo "retry with fastq-dump"
            singularity run -B {",".join(binds)} {container} \
                fastq-dump --outdir /ws/{accession} /inputs/{accession} 
        """)

    out_files = []
    if code == 0:
        extracted_out_dir = OUT_DIR.joinpath(accession)
        for f in os.listdir(extracted_out_dir):
            out_file = f"{f}.tar.gz"
            context.shell(f"""\
                cd {extracted_out_dir}
                tar -cf - {f} | {pigz} -7 -p {P.threads} >{out_file}
            """)
            out_files.append(extracted_out_dir.joinpath(out_file))

        # clean up
        context.shell(f"""\
            rm -r {OUT_DIR.joinpath(TEMP_PREFIX+"*")}
        """)

    return JobResult(
        exit_code = code,
        manifest = {
            EXTRACTED: out_files,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ACCESSION, groupby=ACCESSION)\
    .AddInput(RAW, groupby=ACCESSION)\
    .AddInput(USERNAME, groupby=ACCESSION)\
    .PromiseOutput(EXTRACTED)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=2, memory_gb=16)\
    .SetHome(__file__, name=None)\
    .Build()
