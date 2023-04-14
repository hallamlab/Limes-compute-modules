import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sra accession')
RAW         = Item('sra raw')
USERNAME    = Item('username')
READ_TYPE   = Item('metagenomic read type')
# READ_TYPE is "<type>:<layout>"
# where <type> is one of:
# - "long"
# - "short"
# and <layout> is one of:
# - "paired_end"
# - "interleaved"
# - "single_end"

READS       = Item('metagenomic gzipped reads')

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
    assert isinstance(raw_sra, Path), raw_sra

    read_type = M[READ_TYPE]
    assert isinstance(read_type, str), read_type
    read_length, read_layout = read_type.split(":")

    accession = M[SAMPLE]
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

    # try with fasterq-dump, then fastq-dump if failed
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

    def rm_file_extension(s: str):
        return ".".join(s.split(".")[:-1])

    out_files = []
    if code == 0:
        extracted_out_dir = OUT_DIR.joinpath(accession)
        extracted = list(os.listdir(extracted_out_dir))

        if read_layout == "paired_end":
            fwd_candidates = [f for f in extracted if rm_file_extension(f).endswith("_1")]
            rev_candidates = [f for f in extracted if rm_file_extension(f).endswith("_2")]
            assert len(fwd_candidates) == 1
            assert len(rev_candidates) == 1
            wanted_files = [c[0] for c in (fwd_candidates, rev_candidates)]
        else: # must be only one file
            wanted_files = [f for f in extracted if not any(rm_file_extension(f).endswith(x) for x in ["_1", "_2"])]
            assert len(wanted_files) == 1

        for f in wanted_files:
            out_file = f"{f}.gz"
            context.shell(f"""\
                cd {extracted_out_dir}
                {pigz} -7 -p {P.threads} {f}
            """)
            out_files.append(extracted_out_dir.joinpath(out_file))

        manifest =  {READS: out_files}
    else:
        manifest = {}

    # clean up
    context.shell(f"""\
        rm -r {OUT_DIR.joinpath(TEMP_PREFIX+"*")}
    """)

    return JobResult(
        exit_code = code,
        manifest = manifest,
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(SAMPLE,       groupby=SAMPLE)\
    .AddInput(RAW,          groupby=SAMPLE)\
    .AddInput(USERNAME,     groupby=SAMPLE)\
    .AddInput(READ_TYPE,    groupby=SAMPLE)\
    .PromiseOutput(READS)\
    .Requires({CONTAINER, PIGZ})\
    .SuggestedResources(threads=2, memory_gb=16)\
    .SetHome(__file__, name=None)\
    .Build()
