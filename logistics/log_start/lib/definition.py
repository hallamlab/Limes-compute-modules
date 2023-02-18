from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

TSV = Item('metag samples tsv')
SAMPLE = Item('sample')

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[TSV]

    assert isinstance(input_path, Path), f"input tsv path wasn't path [{input_path}]"

    samples, reads, types = [], [], []
    with open(input_path) as f:
        for l in f:
            if l == '\n': continue
            name, r, type = l.split('\t')
            samples.append(name)
            reads.append(r)
            types.append(type)

    return JobResult(
        exit_code = 0,
        manifest = {
            SAMPLE: samples
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(TSV, groupby=None)\
    .PromiseOutput(SAMPLE)\
    .SetHome(__file__, name=None)\
    .IsLogistical()\
    .Build()
