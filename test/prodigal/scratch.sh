singularity run ../../../lx_ref/prodigal.sif \
    prodigal -p meta \
    -i ./cache/DRR001142.asm.fa -a ./cache/scratch_orfs.faa \
    -o ./cache/scratch_out.gbk -s ./cache/scratch_scores.txt

# singularity run ../../../lx_ref/prodigal.sif \
#     prodigal -h

