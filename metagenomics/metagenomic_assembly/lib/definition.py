import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

SAMPLE = Item('sample')
READS = Item('metagenomic raw reads')
READ_TYPE = Item('metagenomic read type')
# READ_TYPE is "<platform>:<type>"
# where <platform> is one of:
# - "pacbio"
# - "illumina"
# and <type> is one of:
# - "paired_end"
# - "interleaved"
# - "single_end"

ASM = Item('metagenomic assembly')
ASM_WS = Item(f'flye or megahit work')
FLYE = 'flye.sif'
MEGAHIT = 'megahit.sif'

def meta_flye(context: JobContext) -> JobResult:
    params = context.params

    reads = context.manifest[READS]
    rtype = context.manifest[READ_TYPE]
    assert isinstance(rtype, str), f"invalid read type: {rtype}"
    read_source, read_type = rtype.split(':')

    name = context.manifest[SAMPLE]
    assert isinstance(name, str), f"name wasn't a str: {name}"
    output_file_name = f'{name}.asm.fa'

    ws = context.output_folder.joinpath(f"{name}_assembly")
    out = context.output_folder.joinpath(output_file_name)

    def _flye(reads):
        return f"""\
        singularity run -B ./:/ws {context.params.reference_folder}/{FLYE} \
        flye --meta --threads {params.threads} \
            --pacbio-raw /ws/{reads} --out-dir /ws/{ws}
        mv {ws}/assembly.fasta {out}
        """

    def _megahit(reads):
        switch = {
            "paired_end": lambda: f"-1 /ws/{reads[0]} -2 /ws/{reads[1]}",
            "interleaved": lambda: f"-12 /ws/{reads}",
            "single_end": lambda: f"--read /ws/{reads}"
        }
        assert read_type in switch, f"unknown read type [{read_type}]"
        read_param = switch[read_type]()

        return f"""\
        singularity run -B ./:/ws {context.params.reference_folder}/{MEGAHIT} \
        megahit --num-cpu-threads {params.threads} --memory {params.mem_gb}e9\
            {read_param} --out-dir /ws/{ws}
        mv {ws}/final.contigs.fa {out}
        """

    switch = {
        "pacbio": _flye,
        "illumina": _megahit
    }
    assert read_source in switch, f"unknown read source {read_source}"
    exe_cmd = switch[read_source](reads)

    code = context.shell(f"""\
        PYTHONPATH=""
        {exe_cmd}
    """)

    return JobResult(
        exit_code = code,
        manifest = {
            ASM: out,
            ASM_WS: ws
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(meta_flye)\
    .AddInput(SAMPLE,       groupby=SAMPLE)\
    .AddInput(READS,        groupby=SAMPLE)\
    .AddInput(READ_TYPE,    groupby=SAMPLE)\
    .PromiseOutput(ASM)\
    .SuggestedResources(threads=8, memory_gb=16)\
    .Requires({FLYE, MEGAHIT})\
    .SetHome(__file__)\
    .Build()
