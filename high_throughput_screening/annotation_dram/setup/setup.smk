CONTAINER = "dram-annotations.sif"
MAG_ANNOTATOR = "mag_annotator"
MAG_ANNOTATOR_CONFIG = f"{MAG_ANNOTATOR}/CONFIG"
DATABASE = "dram_dbs"
DATABASE_PROXY = f"{DATABASE}/.mark_complete"

rule singularity:
    input:
        CONTAINER,
        DATABASE_PROXY

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity build {output} docker://quay.io/hallam_lab/dram-annotations:1.4.6
        """

rule setup_mag_annotator:
    input:
        CONTAINER
    output:
        MAG_ANNOTATOR_CONFIG
    shell:
        """
        singularity run -B ./:/ws {input} \
            cp -r /opt/conda/envs/dram/lib/python3.10/site-packages/mag_annotator /ws
        """

# https://github.com/WrightonLabCSU/DRAM
# skipping uniref by default!
# this bit is hard to run, requiring >500GB of RAM if using uniref90
# this also doesn't really work on networked filesystems
# see example hpc 
rule setup_databases:
    input:
        image=CONTAINER,
        mount=MAG_ANNOTATOR_CONFIG
    output:
        DATABASE_PROXY
    params:
        maga=MAG_ANNOTATOR,
        db=DATABASE
    threads: 8
    shell:
        """
        singularity run -B ./{params.db}:/ref,./:/ws,./{params.maga}:/opt/conda/envs/dram/lib/python3.10/site-packages/mag_annotator {input.image} \
            DRAM-setup.py prepare_databases --threads {threads} --skip_uniref --output_dir /ref \
        && touch {output}
        """
