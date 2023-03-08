VERSION = "3.0.1"
CONTAINER = "sratk.sif"

# todo: add pigz

rule singularity:
    input:
        CONTAINER

rule get_image:
    output:
        CONTAINER
    params:
        ver=VERSION
    shell:
        """
        singularity build {output} docker://ncbi/sra-tools:{params.ver}
        """
