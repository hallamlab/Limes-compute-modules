#!/bin/bash
OWS=/home/phyberos/scratch/dram_setup
out=/home/phyberos/scratch/dram_dbs

cd $SLURM_TMPDIR
mkdir dram_setup_ws
cd dram_setup_ws
pwd

echo copying...
date
cp -r $OWS/* ./
echo starting...
date

singularity run -B ./:/ws,./mag_annotator:/opt/conda/envs/dram/lib/python3.10/site-packages/mag_annotator ./dram-annotations.sif \
    DRAM-setup.py prepare_databases --threads 32 --output_dir /ws

echo copying back
date
mkdir -p $out
cp -r * $out/

echo done
date
