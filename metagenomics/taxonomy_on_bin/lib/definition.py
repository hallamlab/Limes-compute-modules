from limes_x import ModuleBuilder, Item
from gtdbtk import gtdbtk_procedure

SAMPLE      = Item('sra accession')
BINS        = Item('metagenomic bin')

GTDBTK_WS   = Item('bin gtdbtk work')
GTDBTK_TAX  = Item('bin taxonomy table')

CONTAINER   = 'gtdbtk.sif'
GTDBTK_DB   = 'gtdbtk_data'

MODULE = ModuleBuilder()\
    .SetProcedure(lambda context: gtdbtk_procedure(context, SAMPLE, BINS, GTDBTK_WS, GTDBTK_TAX, CONTAINER, GTDBTK_DB))\
    .AddInput(SAMPLE)\
    .AddInput(BINS, groupby=SAMPLE)\
    .PromiseOutput(GTDBTK_WS)\
    .PromiseOutput(GTDBTK_TAX)\
    .SuggestedResources(threads=1, memory_gb=48)\
    .Requires({CONTAINER, GTDBTK_DB})\
    .SetHome(__file__, name=None)\
    .Build()
