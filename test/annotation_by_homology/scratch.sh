date

singularity run ../../../lx_ref/diamond.sif \
    blastp --very-sensitive --threads 12 --memory-limit 8 \
    --db ../../../lx_ref/eggnog_proteins.dmnd \
    --outfmt 6 qseqid stitle qstart qend nident pident evalue \
    --query ./cache/scratch_orfs.faa \
    --out ./cache/scratch_hits.eg.tsv

date
# singularity run ../../../lx_ref/diamond.sif \
#     help

#     | grep "Download All Data" | head -n 1 \
#     | cut -c 73- | rev | cut -c 24- | rev | sed --expression='s/amp;//g')
# wget "$link" -O x
