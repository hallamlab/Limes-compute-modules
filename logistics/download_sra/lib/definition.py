from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

ACCESSION   = Item('sra accession')
USERNAME    = Item('username')

RAW         = Item('sra raw')

CONTAINER   = 'sratk.sif'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    OUT_DIR = context.output_folder
    TEMP_PREFIX = "temp"
    container = P.reference_folder.joinpath(CONTAINER)

    accession_list = M[ACCESSION]
    if not isinstance(accession_list, list):
        accession_list = [accession_list]
    for acc in accession_list:
        assert isinstance(acc, str), f"expected str for accession but got {acc}"

    user = M[USERNAME]
    assert isinstance(user, str)

    fake_home = OUT_DIR.joinpath(f"{TEMP_PREFIX}.home")
    context.shell(f"mkdir -p {fake_home}")
    binds = [
        f"{fake_home}:/home/{user}",
        f"{OUT_DIR}:/ws",
    ]

    for acc in accession_list:
        context.shell(f"""\
            singularity run -B {",".join(binds)} {container} \
                prefetch --output-directory /ws/ {acc}
        """)

    return JobResult(
        manifest = {
            RAW: [OUT_DIR.joinpath(acc) for acc in accession_list]
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(ACCESSION, groupby=ACCESSION)\
    .AddInput(USERNAME, groupby=ACCESSION)\
    .PromiseOutput(RAW)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=1, memory_gb=12)\
    .SetHome(__file__, name=None)\
    .Build()
