CONTAINER = "kraken2.sif"
REF_DB = "k2_standard_16gb_20230314"
REF_DB_FILE = f"{REF_DB}/hash.k2d"
REF_DB_ZIP = f"{REF_DB}.tar.gz"
PIGZ = "pigz"

rule singularity:
    input:
        PIGZ,
        CONTAINER,
        REF_DB_FILE

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity build {output} docker://staphb/kraken2:2.1.2-no-db
        """

rule download_refdata:
    output:
        REF_DB_ZIP
    shell:
        """
        wget --quiet https://genome-idx.s3.amazonaws.com/kraken/{output}
        """

rule extract_refdata:
    input:
        REF_DB_ZIP
    output:
        REF_DB_FILE
    params:
        db=REF_DB
    shell:
        """
        mkdir -p {params.db}
        tar -xf {input} -C {params.db}
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
        && chmod +x pigz && cp pigz ../ && cd ../ && rm -rf pigz_lib \
        && rm {input}
        """