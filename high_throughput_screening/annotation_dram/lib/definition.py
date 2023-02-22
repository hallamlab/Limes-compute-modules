from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sample')
ASSEMBLIES  = Item('fosmid assembly')
USE_UNIREF  = Item('use uniref')

ANNOTATIONS = Item('dram annotation')
DRAM_WS     = Item('dram work')

CONTAINER   = "dram-annotations.sif"
DRAM_DBS    = "dram_dbs"

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    REF = context.params.reference_folder
    OUT = context.output_folder
    container = REF.joinpath(CONTAINER)
    TEMP_PREFIX = "temp"

    sample = M[SAMPLE]
    assert isinstance(sample, str), f"expected str, got {sample}"
    WS = OUT.joinpath(f'{sample}_dram')

    genomes = M[ASSEMBLIES]
    if not isinstance(genomes, list): genomes = [genomes]
    inputs = OUT.joinpath(f"{TEMP_PREFIX}.genomes")
    context.shell(f"mkdir -p {inputs}")
    ext = 'fa'
    for g in genomes:
        assert isinstance(g, Path), f"expected path for genome, got {g}"
        fname = '.'.join(g.name.split('.')[:-1])
        context.shell(f"cp -L {g} {inputs}/{fname}.{ext}")

    _use_uniref = M[USE_UNIREF]
    assert isinstance(_use_uniref, str), f"expected str, got {_use_uniref}"
    _use_uniref = _use_uniref.lower()
    use_uniref = "--use_uniref" if _use_uniref == "true" or _use_uniref == "1" else ""
    if use_uniref != "":
        assert P.mem_gb >= 220, f"not enough memory given"

    binds = [
        f"{REF.joinpath(DRAM_DBS)}:/ref",
        f"./:/ws",
    ]

    code = context.shell(f"""\
        singularity run -B {",".join(binds)} {container} \
            DRAM.py annotate --threads {P.threads} \
            --min_contig_size 2000 --prodigal_mode meta {use_uniref} \
            -i '/ws/{inputs}/*.{ext}' -o /ws/{WS}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            ANNOTATIONS: [], # todo
            DRAM_WS: WS,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(SAMPLE,)\
    .AddInput(ASSEMBLIES, groupby=SAMPLE)\
    .PromiseOutput(ANNOTATIONS)\
    .Requires({DRAM_DBS, CONTAINER})\
    .SuggestedResources(threads=16, memory_gb=58)\
    .SetHome(__file__, name=None)\
    .Build()
