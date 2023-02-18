import os
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

TSV = Item('metag samples tsv')
SAMPLE = Item('sample')
READS = Item('metagenomic raw reads')
READ_TYPE = Item('metagenomic read type')

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[TSV]
    assert isinstance(input_path, Path), f"input tsv path wasn't path {input_path}"
    sample = context.manifest[SAMPLE]
    assert isinstance(sample, str), f"sample name wasn't str {sample}"

    reads, type = [], ""
    with open(input_path) as f:
        for l in f:
            l = l[:-1]
            if l == '': continue
            name, r, type = l.split('\t')
            if name != sample: continue
            reads = [Path(p) for p in r.split(';')]
            break

    linked_reads: list[Path] = []
    for r in reads:
        assert r.exists(), f"can't find this file {r}"
        fname = str(r).split('/')[-1]
        linked = context.output_folder.joinpath(fname)
        os.symlink(r, linked)
        linked_reads.append(linked)

    return JobResult(
        exit_code = 0,
        manifest = {
            READS: linked_reads,
            READ_TYPE: type,
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(TSV)\
    .AddInput(SAMPLE)\
    .PromiseOutput(READS)\
    .PromiseOutput(READ_TYPE)\
    .SetHome(__file__, name=None)\
    .IsLogistical()\
    .Build()
