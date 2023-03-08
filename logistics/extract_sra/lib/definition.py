import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

ACCESSION   = Item('sra accession')
RAW         = Item('sra raw')

EXTRACTED   = Item('sra extracted')
SNAPSHOT    = Item('folder snapshot')

CONTAINER   = 'sratk.sif'
# and pigz...

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    OUT_DIR = context.output_folder
    TEMP_PREFIX = "temp"
    container = P.reference_folder.joinpath(CONTAINER)

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

    binds = [
        f"{fake_home}:/home/{os.getlogin()}",
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

    out_file = f"{accession}.tar.gz"
    snapshot = f"{accession}.snapshot.txt"
    if code == 0:
        context.shell(f"""\
            cd {OUT_DIR}
            ls -lh {accession} > {snapshot}
            tar -cf - {accession} | pigz -7 -p {P.threads} >{out_file}\
            && rm -r {accession}
        """)

    # clean up
    context.shell(f"""\
        rm -r {OUT_DIR.joinpath(TEMP_PREFIX+"*")}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            EXTRACTED: OUT_DIR.joinpath(out_file),
            SNAPSHOT: OUT_DIR.joinpath(snapshot),
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ACCESSION, groupby=ACCESSION)\
    .AddInput(RAW, groupby=ACCESSION)\
    .PromiseOutput(EXTRACTED)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=2, memory_gb=16)\
    .SetHome(__file__, name=None)\
    .Build()
