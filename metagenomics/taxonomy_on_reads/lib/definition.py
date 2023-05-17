from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sra accession') # this is just used for grouping, todo: remove
READS       = Item('metagenomic gzipped reads')

REPORT      = Item('reads taxonomy table')
HITS        = Item('gzipped taxonmic hits of reads')

CONTAINER   = 'kraken2.sif'
_DB_SIZE = 16
# _DB_SIZE = 8
REF_DB      = f'k2_standard_{_DB_SIZE:02}gb_20230314' # 8gb db is named "...08gb..."
PIGZ        = 'pigz'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    WS = context.output_folder
    REF = P.reference_folder
    TEMP = "TEMP"

    sample = M[SAMPLE]
    assert isinstance(sample, str), f"expected str for sample, got: {sample}"

    _reads = M[READS]
    input_dir = WS.joinpath(f"{TEMP}.inputs")
    context.shell(f"mkdir -p {input_dir}")
    assert not isinstance(_reads, str), f"got string instead of path for reads: {_reads}"
    if not isinstance(_reads, list): _reads = [_reads]
    reads = []
    for r in _reads:
        assert isinstance(r, Path), f"expected path for reads, got: {r}"
        context.shell(f"cp -L {r.absolute()} {input_dir.joinpath(r.name)}")
        reads.append(r.name)

    binds = [
        f"{REF}/:/ref",
        f"{input_dir}:/inputs",
        f"{WS}:/ws",
    ]

    reports_name = f"{sample}.report.krkn"
    hits_name = f"{sample}.hits.krkn"
    hits_zipped = f"{hits_name}.gz"

    context.shell(f"""\
        singularity run -B {",".join(binds)} {REF.joinpath(CONTAINER)} kraken2 \
            --threads {P.threads} --gzip-compressed {'--memory-mapping' if P.mem_gb<_DB_SIZE else ""} \
            --db /ref/{REF_DB} \
            --use-names --report /ws/{reports_name} \
            {' '.join([f'/inputs/{r}' for r in reads])} \
        | {REF.joinpath(PIGZ)} -7 -p {P.threads} >{WS.joinpath(hits_zipped)}
        rm -r {WS}/{TEMP}*
    """)

    return JobResult(
        manifest={
            REPORT: WS.joinpath(reports_name),
            HITS: WS.joinpath(hits_zipped),
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(SAMPLE,       groupby=SAMPLE)\
    .AddInput(READS,        groupby=SAMPLE)\
    .PromiseOutput(REPORT)\
    .PromiseOutput(HITS)\
    .SuggestedResources(threads=6, memory_gb=20)\
    .Requires({CONTAINER, REF_DB, PIGZ})\
    .SetHome(__file__, name=None)\
    .Build()
