import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

# split this...

SAMPLE      = Item('sra accession')
READS       = Item('metagenomic gzipped reads')
READ_TYPE   = Item('metagenomic read type')
ASM         = Item('metagenomic assembly')

BIN         = Item('metagenomic bin')
BIN_STATS   = Item('metagenomic binning stats')
MWR_WS      = Item('metawrap binning work')
MWR_REFINE_WS = Item('metawrap refine work')

CONTAINER   = 'metawrap.sif'
CHECKM_DB   = 'checkm_data_2015_01_16'
CHECKM_SRC  = 'checkm_src'

def example_procedure(context: JobContext) -> JobResult:
    COMPLETION, CONTAMINATION = 50, 5
    TEMP_PREFIX = "temp"
    cache = context.output_folder.joinpath(TEMP_PREFIX)
    os.makedirs(cache, exist_ok=True)
    def fail(msg: str):
        return JobResult(
            error_message = msg,
        )

    params = context.params
    ref = params.reference_folder
    binds = [
        f"{ref}/{CHECKM_SRC}:/usr/local/lib/python2.7/site-packages/checkm",
        f"{ref}/{CHECKM_DB}:/checkm_db",
        f"./:/ws",
    ]

    _reads = context.manifest[READS]
    if not isinstance(_reads, list): _reads = [_reads]
    zipped_reads: list[Path] =[]
    for r in _reads:
        assert isinstance(r, Path), f"expected path for reads, got {type(r)} for {r}"
        zipped_reads.append(r)

    rtype = context.manifest[READ_TYPE]
    assert isinstance(rtype, str), f"invalid read type: {rtype}"
    _, read_type = rtype.split(':')

    name = context.manifest[SAMPLE]
    assert isinstance(name, str), f"name wasn't a str: {name}"

    asm = context.manifest[ASM]
    assert isinstance(asm, Path), f"assembly wasn't a path: {name}"

    special_read_type = { # switch
        "interleaved": lambda: "--interleaved",
        "single_end": lambda: "--single-end",
        "long_read": lambda: "--single-end",
    }.get(read_type, lambda: "")()
    container = params.reference_folder.joinpath(CONTAINER)

    #################################################################################
    # extract reads
    reads: list[Path] = []
    for r in zipped_reads:
        unzipped_r = cache.joinpath(r.name.replace(".gz", ""))
        context.shell(f"gunzip -c {r} >{unzipped_r}")
        reads.append(unzipped_r)

    #################################################################################
    # bin

    metawrap_out = context.output_folder.joinpath(f'{name}_metawrap')
    code = context.shell(f"""\
        PYTHONPATH=""
        singularity exec -B {",".join(binds)} {container} \
        metaWRAP binning -t {params.threads} -m {params.mem_gb} --maxbin2 --metabat2 --concoct {special_read_type} \
            -a /ws/{asm} \
            -o /ws/{metawrap_out} \
            {" ".join(str(r) for r in reads)}
    """)
    if code != 0: return fail("metawrap binning failed")
    
    #################################################################################
    # refine

    refine_out = context.output_folder.joinpath(f'{name}_metawrap_refine')
    refined_bins = refine_out.joinpath(f'metawrap_{COMPLETION}_{CONTAMINATION}_bins')
    bin_folders = [
        f for f in os.listdir(metawrap_out) 
        if '_bins' in f and len([b for b in os.listdir(f'{metawrap_out}/{f}') if "unbinned" not in b])>0
    ]
    ABC = 'ABC'
    if len(bin_folders)>1:
        code = context.shell(f"""\
            PYTHONPATH=""
            singularity exec -B {",".join(binds)} {container} \
            metaWRAP bin_refinement -t {params.threads} -m {params.mem_gb} --quick  \
                -c {COMPLETION} -x {CONTAMINATION} \
                {" ".join([f'-{ABC[i]} /ws/{metawrap_out}/{f}' for i, f in enumerate(bin_folders)])} \
                -o /ws/{refine_out}
        """)
    elif len(bin_folders)==1:
        code = context.shell(f"""\
            mkdir -p {refine_out}
            cp -R {metawrap_out}/{bin_folders[0]} {refined_bins}
        """)
    else:
        return fail("metawrap binning produced no bins")
    if code != 0: return fail("metawrap bin refinement failed")

    original_bins = os.listdir(refined_bins)
    renamed_bins = [f'{name}_bin{i:02}.fa' for i, _ in enumerate(original_bins)]
    renamed_bin_paths = [context.output_folder.joinpath(b) for b in renamed_bins]
    rename_map = {}
    with open(refine_out.joinpath('bin_rename_mapping.tsv'), 'w') as f:
        for ori, new in zip(original_bins, renamed_bins):
            f.write(f'{ori}\t{new}\n')
            rename_map[ori] = new

    #################################################################################
    # extract stats
    def get_binners(entry: str):
        letters = entry.replace("bins", "")
        binners = []
        for letter in letters:
            i = ABC.index(letter)
            binners.append(bin_folders[i].replace("_bins", ""))
        return ";".join(binners)

    def get_new_name(bin: str):
        return rename_map[f"{bin}.fa"].replace(f"{name}_", "").replace(f".fa", "").replace("bin", "")

    TAB = '\t'
    stats_file = context.output_folder.joinpath(f"{name}.stats")
    with open(refine_out.joinpath("metawrap_50_5_bins.stats")) as original:
        with open(stats_file, 'w') as stats:
            header = original.readline()
            stats.write(header)
            for l in original:
                tokens = l[:-1].split("\t")
                new_tokens = [get_new_name(tokens[0])] + tokens[1:-1] + [get_binners(tokens[-1])]
                stats.write(TAB.join(new_tokens))

    #################################################################################
    # cleanup
    NL = '\n'
    context.shell(f"""\
        {NL.join(f"cp {refined_bins.joinpath(o)} {n}" for o, n in zip(original_bins, renamed_bin_paths))}
        rm -rf {cache}
    """)

    return JobResult(
        manifest = {
            BIN: renamed_bin_paths,
            BIN_STATS: stats_file,
            MWR_WS: metawrap_out,
            MWR_REFINE_WS: refine_out,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(example_procedure)\
    .AddInput(SAMPLE,       groupby=SAMPLE)\
    .AddInput(READS,        groupby=SAMPLE)\
    .AddInput(READ_TYPE,    groupby=SAMPLE)\
    .AddInput(ASM,          groupby=SAMPLE)\
    .PromiseOutput(BIN)\
    .PromiseOutput(MWR_WS)\
    .PromiseOutput(MWR_REFINE_WS)\
    .Requires({CONTAINER})\
    .Requires({CHECKM_DB})\
    .Requires({CHECKM_SRC})\
    .SuggestedResources(threads=2, memory_gb=48)\
    .SetHome(__file__)\
    .Build()
