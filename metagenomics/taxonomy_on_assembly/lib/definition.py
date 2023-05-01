from limes_x import ModuleBuilder, Item
from gtdbtk import gtdbtk_procedure, GTDBTK_DB

SAMPLE      = Item('sra accession')
BINS        = Item('metagenomic assembly')

GTDBTK_WS   = Item('assembly gtdbtk work')
GTDBTK_TAX  = Item('assembly taxonomy table')

CONTAINER   = 'gtdbtk.sif'

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
