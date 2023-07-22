import os
from pathlib import Path

from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE      = Item('sra accession')
READS       = Item('metagenomic gzipped reads')

ASMBLER     = Item('assembler')
ASM         = Item('metagenomic assembly')
ASM_WS      = Item('metagenomic assembly work')

FLYE        = 'flye.sif'
MEGAHIT     = 'megahit.sif'
PIGZ        = 'pigz'

# assumes either short or long reads, hybrid assembly not implimented
# infers type from average read length > 600
def procedure(context: JobContext) -> JobResult:
    params = context.params
    print = lambda x: context.shell(f'echo "{x}"')

    _reads = context.manifest[READS]
    if not isinstance(_reads, list):
        _reads = [_reads]
    reads: list[Path] = []
    for r in _reads:
        assert isinstance(r, Path), f"expected path for reads, got {r}"
        reads.append(r)
    print(f"{len(reads)} reads")

    name = context.manifest[SAMPLE]
    assert isinstance(name, str), f"name wasn't a str: {name}"
    output_file_name = f'{name}.asm.fa'

    ws = context.output_folder.joinpath(f"{name}_assembly")
    out = context.output_folder.joinpath(output_file_name)

    def _flye(reads: list[Path]):
        print("using flye")
        return f"""\
        singularity run -B ./:/ws {context.params.reference_folder}/{FLYE} \
        flye --meta --threads {params.threads} \
            --pacbio-raw {' '.join('/ws/'+str(r) for r in reads)} --out-dir /ws/{ws}
        mv {ws}/assembly.fasta {out}
        """, "flye"

    def _megahit(reads: list[Path]):
        ones, twos, singles = [], [], []
        for rpath in reads:
            name = rpath.name.replace(".gz", "").replace(".fastq", "")
            r = f"/ws/{rpath}"
            if name.endswith("_1"):
                ones.append(r)
            elif name.endswith("_2"):
                twos.append(r)
            else:
                singles.append(r)

        assert len(ones) == len(twos), f"number of paired reads don't match 1:{len(ones)},2:{len(twos)}"

        print("using megahit")
        print(f"{len(ones)} paired reads, {len(singles)} singles")
        read_params = []
        if len(ones)>0:
            read_params.append(f"-1 {','.join(ones)} -2 {','.join(twos)}")
        if len(singles)>0:
            read_params.append(f"-r {','.join(singles)}")
        return f"""\
        singularity run -B ./:/ws {context.params.reference_folder}/{MEGAHIT} \
        megahit --num-cpu-threads {params.threads} --memory {params.mem_gb}e9\
            {' '.join(read_params)} --out-dir /ws/{ws}
        mv {ws}/final.contigs.fa {out}
        """, "megahit"

    #https://bioinformatics.stackexchange.com/questions/935/fast-way-to-count-number-of-reads-and-number-of-bases-in-a-fastq-file
    read_sizes = context.output_folder.joinpath("temp.readcount.txt")
    num_reads, nucleotides = 0, 0
    for r in reads:
        context.shell(f"""\
            {context.params.reference_folder.joinpath(PIGZ)} -p {params.threads} -dc {r} \
            | awk 'NR % 4 == 2' \
            | wc -cl >{read_sizes} \
        """)
        with open(read_sizes) as f:
            toks = f.readline()[:-1].strip()
            if "\t" in toks: toks = toks.split("\t")
            else: toks = [t for t in toks.split(" ") if len(t)>0]
            nr, nuc = [int(t) for t in toks]
            num_reads += nr
            nucleotides += nuc

    # todo: use metadata from sra
    is_short_read = nucleotides/num_reads < 5_000

    if is_short_read:
        exe_cmd, assembler = _megahit(reads)
    else:
        exe_cmd, assembler = _flye(reads)
    code = context.shell(f"""\
        PYTHONPATH=""
        {exe_cmd}
        rm {read_sizes}
    """)

    return JobResult(
        manifest = {
            ASM: out,
            ASMBLER: assembler,
            ASM_WS: ws,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(SAMPLE,       groupby=SAMPLE)\
    .AddInput(READS,        groupby=SAMPLE)\
    .PromiseOutput(ASM)\
    .PromiseOutput(ASMBLER)\
    .SuggestedResources(threads=8, memory_gb=16)\
    .Requires({FLYE, MEGAHIT, PIGZ})\
    .SetHome(__file__)\
    .Build()
