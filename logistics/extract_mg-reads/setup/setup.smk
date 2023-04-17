VERSION = "3.0.1"
CONTAINER = "sratk.sif"
PIGZ = "pigz"

rule singularity:
    input:
        CONTAINER,
        PIGZ

rule get_image:
    output:
        CONTAINER
    params:
        ver=VERSION
    shell:
        """
        singularity build {output} docker://ncbi/sra-tools:{params.ver}
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
