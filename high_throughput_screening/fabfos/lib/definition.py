import os
import re
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sample')
HITS        = Item('fosmid hits')
PARITY      = Item('read parity')
BACKGROUND  = Item('fosmid background')
ASSEMBLER   = Item('assembler name')
COUNT       = Item('fosmid count estimate')

ASSEMBLY    = Item('fosmid assembly')
FILTERED    = Item('filtered fosmid hits')
COVERAGE    = Item('fosmid read coverage')

CONTAINER   = 'fabfos.sif'

def procedure(context: JobContext) -> JobResult:
    P = context.params
    M = context.manifest
    OUT_DIR = context.output_folder
    TEMP_PREFIX = "temp"
    container = P.reference_folder.joinpath(CONTAINER)
    
    sample = M[SAMPLE]
    sample = re.sub(r'[^\w]+', "_", str(sample))

    assembler_name = M[ASSEMBLER]
    ASM_CHOICES = {"megahit", "spades"}
    assert assembler_name in ASM_CHOICES, f"assembler must be one of: {'/'.join(ASM_CHOICES)}, but got {assembler_name}"

    reads = M[HITS]
    if not isinstance(reads, list):
        reads = [reads]
    for r in reads:
        assert isinstance(r, Path), f"expected a path for each read but got: {r}"
    reads = sorted(reads)

    parity = M[PARITY]
    assert isinstance(parity, str), f"expected a str for parity, but got {parity}"
    PE, I, SE = "paired_end", "interleaved", "single_end"
    PAR_CHOICES = {PE, I, SE}
    assert parity in PAR_CHOICES, f"parity must be one of {'/'.join(PAR_CHOICES)}, but got {parity}"

    bg = M[BACKGROUND]
    assert isinstance(bg, Path), f"expected path for background, got {bg}"

    count = M[COUNT]
    assert isinstance(count, str), f"expected a str for count, got {count}"
    try:
        count = int(count)
    except Exception:
        count = 384
        print(f"defaulting fosmid count estimate to {count}")

    # sort out reads & parity
    reads_dir = OUT_DIR.joinpath(f"{TEMP_PREFIX}.reads"); os.makedirs(reads_dir)
    os.symlink(f'../../{reads[0]}', reads_dir.joinpath(f"{sample}.fastq"))
    if parity == PE:
        assert len(reads) == 2, f"expected 2 reads for {PE}, got {reads}"
        rev_dir = OUT_DIR.joinpath(f"{TEMP_PREFIX}.rev"); os.makedirs(rev_dir)
        rev_path = rev_dir.joinpath(f"{sample}.fastq")
        os.symlink(f'../../{reads[1]}', rev_path)
        reads_arg = f"-r /ws/{reads_dir} -2 /ws/{rev_dir} -p pe"
    elif parity == I:
        reads_arg = f"-r /ws/{reads_dir} -i"
    else: # parity == SE
        reads_arg = f"-r /ws/{reads_dir} -p se"

    # estimate read length
    with open(reads[0]) as r:
        rcount = 0
        s = 0
        for i, l in enumerate(r):
            if i % 4096 == 1:
                rcount += len(l)-1
                s += 1
        read_len_estimate = int(round(rcount/s))

    # move background
    bg_dir = OUT_DIR.joinpath(f"{TEMP_PREFIX}.bg"); os.makedirs(bg_dir)
    bg_linked = bg_dir.joinpath('background.fasta')
    os.symlink(f'../../{bg}', bg_linked)

    # make a miffed file
    miff_header = "#Sample Name (LLLLL-PP-WWW),Project,Human selector,Vector Name,Screen [in silico | functional],Selection criteria,Number of fosmids,Sequencing submission date (YYYY-MM-DD),Glycerol plate name,Sequencing center,Sequencing type,Read length,Instrument"
    miff_file = OUT_DIR.joinpath(f"{TEMP_PREFIX}.miffed.csv")
    fabfos_workspace = f"{sample}_ws"
    with open(miff_file, 'w') as miffed:
        entry = [sample, fabfos_workspace]+["_"]*11
        entry[4] = "in silico"
        entry[6] = f"{count}"
        entry[11] = f"{read_len_estimate}"
        miffed.writelines(l+'\n' for l in [
            miff_header,
            ",".join(entry),
            ""
        ])

    binds = [
        f"./:/ws",
    ]

    contigs = OUT_DIR.joinpath(f"{sample}_contigs.fa")
    filtered = OUT_DIR.joinpath(f"{sample}_filtered")
    final_out = OUT_DIR.joinpath(fabfos_workspace, sample)
    os.makedirs(filtered)
    context.shell(f"""\
        singularity run -B {",".join(binds)} {container} \
        fabfos --threads {P.threads} --fabfos_path /ws/{OUT_DIR} --force \
            --assembler {assembler_name} -m /ws/{miff_file} {reads_arg} -b /ws/{bg_linked}
        mv {final_out}/*contigs.fasta {contigs}
        mv {final_out}/*1.fastq {filtered}/
        mv {final_out}/*2.fastq {filtered}/
    """)

    with open(final_out.joinpath(f"{sample}_estimated_coverage.txt")) as f:
        cov = f.readline()[:-1]

    context.shell(f"""\
        rm {OUT_DIR}/FabFos_master_metadata.tsv
        rm -r {OUT_DIR}/{TEMP_PREFIX}*
        rm -r {OUT_DIR.joinpath(fabfos_workspace)}
    """)

    return JobResult(
        exit_code = 0,
        manifest = {
            ASSEMBLY: contigs,
            COVERAGE: str(cov),
            FILTERED: filtered,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(SAMPLE)\
    .AddInput(ASSEMBLER, groupby=SAMPLE)\
    .AddInput(HITS, groupby=SAMPLE)\
    .AddInput(PARITY, groupby=SAMPLE)\
    .AddInput(BACKGROUND, groupby=SAMPLE)\
    .AddInput(COUNT, groupby=SAMPLE)\
    .PromiseOutput(ASSEMBLY)\
    .PromiseOutput(COVERAGE)\
    .PromiseOutput(FILTERED)\
    .Requires({CONTAINER})\
    .SuggestedResources(threads=14, memory_gb=16)\
    .SetHome(__file__, name=None)\
    .Build()
