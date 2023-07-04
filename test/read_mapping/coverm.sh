

singularity build ./cache/coverm.sif docker://marcodelapierre/coverm

# CM_CACHE="./cache/coverm_bams"
# mkdir -p $CM_CACHE

# coverm contig -p minimap2-no-preset -m tpm \
#     --single ./cache/cy/reads/SRR10053317/SRR10053317.fastq.gz \
#     --reference ./cache/cy/asms/SRR10053317.asm.fa \
#     -o ./cache/SRR10053317.coverm \
#     --bam-file-cache-directory $CM_CACHE \
#     --no-zeros --output-format dense \
#     -t 12 2>./cache/SRR10053317.log
