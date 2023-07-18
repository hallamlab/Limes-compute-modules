# singularity build fastqc.sif docker://pegi3s/fastqc:latest

date

singularity exec -B ./cache:/ws ./cache/fastqc.sif \
    fastqc --noextract -o /ws /ws/SRR10140508_1.fastq.gz

date
# singularity run ../../../lx_ref/diamond.sif \
#     help

#     | grep "Download All Data" | head -n 1 \
#     | cut -c 73- | rev | cut -c 24- | rev | sed --expression='s/amp;//g')
# wget "$link" -O x
