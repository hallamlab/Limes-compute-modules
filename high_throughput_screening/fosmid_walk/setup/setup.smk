CONTAINER = "foswalk.sif"

rule singularity:
    input:
        CONTAINER

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity build {output} docker://quay.io/hallam_lab/fosmid-walk
        """
