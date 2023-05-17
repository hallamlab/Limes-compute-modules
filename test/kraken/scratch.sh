# docker run --rm \
#     --mount type=bind,source="/home/tony/workspace/python/Limes-all/Limes-compute-modules/scratch/kraken/cache/",target="/ws" \
#     staphb/kraken2:latest kraken2 \
#     --threads 6 \
#     --db /ws/k2_standard_08gb_20230314 \
#     --gzip-compressed \
#     --use-names --report /ws/testrep \
#     --output /ws/testout \
#     /ws/SRR10140508_1.fastq.gz

docker run --rm \
    staphb/kraken2:latest kraken2 \
    --help
