from limes_x import ModuleBuilder, Item
from gtdbtk import gtdbtk_procedure, GTDBTK_DB

SAMPLE      = Item('sra accession')
BINS        = Item('metagenomic bin')

GTDBTK_WS   = Item('bin gtdbtk work')
GTDBTK_TAX  = Item('bin taxonomy table')

PIGZ        = 'pigz'
CONTAINER   = 'gtdbtk.sif'

MODULE = ModuleBuilder()\
    .SetProcedure(lambda context: gtdbtk_procedure(context, SAMPLE, BINS, GTDBTK_WS, GTDBTK_TAX, CONTAINER, GTDBTK_DB, PIGZ))\
    .AddInput(SAMPLE)\
    .AddInput(BINS, groupby=SAMPLE)\
    .PromiseOutput(GTDBTK_WS)\
    .PromiseOutput(GTDBTK_TAX)\
    .SuggestedResources(threads=2, memory_gb=48)\
    .Requires({CONTAINER, GTDBTK_DB, PIGZ})\
    .SetHome(__file__, name=None)\
    .Build()
