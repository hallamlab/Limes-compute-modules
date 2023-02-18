rule singularity:
    input:
        "flye.sif",
        "megahit.sif"

rule flye:
    output:
        "flye.sif"
    shell:
        """
        singularity pull {output} library://ff_phil/default/assembly_flye_2.9:v0.1
        """
        
rule megahit:
    output:
        "megahit.sif"
    shell:
        """
        singularity pull {output} library://txyliu/limesx/docker_vout_megahit:release-v1.2.9
        """
