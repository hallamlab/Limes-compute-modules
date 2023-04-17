PIGZ = "pigz"

rule singularity:
    input:
        "flye.sif",
        "megahit.sif",
        PIGZ

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

rule get_pigz:
    output:
        "pigz.tar.gz"
    shell:
        """
        wget https://zlib.net/pigz/{output}
        """

rule compile_pigz:
    output:
        PIGZ
    input:
        "pigz.tar.gz"
    shell:
        """
        tar -xf {input} && mv pigz pigz_lib \
        && cd pigz_lib && make \
        && cp pigz ../ && cd ../ && rm -rf pigz_lib \
        && rm {input}
        """
