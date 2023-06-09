CONTAINER = "prodigal.sif"

rule singularity:
    input:
        CONTAINER

rule prodigal:
    output:
        CONTAINER
    shell:
        """
        singularity pull {output} docker://biocontainers/prodigal:v1-2.6.3-4-deb_cv1
        """
