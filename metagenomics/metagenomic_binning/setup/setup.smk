CHECKM_DB = "checkm_data_2015_01_16"
CHECKM_SRC = "checkm_src"
PIGZ = "pigz"

rule singularity:
    input:
        "metawrap.sif",
        PIGZ,
        CHECKM_SRC+"/__init__.py",  # we want the whole folder, but snakemake prefers files over folders
        CHECKM_DB+"/.dmanifest"     # again, we pick a file to represent the folder

rule metawrap:
    output:
        "metawrap.sif"
    shell:
        """
        singularity pull {output} library://txyliu/limesx/quay_biocontainers_metawrap-mg:v1.3.0
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

# the last bind is to ensure that the default db location really doesn't exist, in the event of a crazy fluke
rule checkm_src:
    input:
        container="metawrap.sif",
        db=CHECKM_DB+"/.dmanifest"
    output:
        CHECKM_SRC+"/__init__.py"
    params:
        src=CHECKM_SRC,
        db=CHECKM_DB
    shell:
        """
        [ -d "{params.src}" ] && rm -r {params.src}
        singularity exec -B ./:/ws {input.container} \
            cp -R /usr/local/lib/python2.7/site-packages/checkm /ws/{params.src} \
        && singularity exec -B ./:/ws,./{params.src}:/usr/local/lib/python2.7/site-packages/checkm,./{params.db}:/checkm_db,./:/srv/whitlam/bio/db/checkm_data/1.0.0 {input.container} \
            /bin/bash -c "echo /checkm_db | checkm" \
        || rm -r {params.src}
        """

rule download_checkm_db:
    output:
        "temp.%s.tar.gz" % CHECKM_DB
    shell:
        """
        wget --quiet https://data.ace.uq.edu.au/public/CheckM_databases/%s.tar.gz
        mv %s.tar.gz {output}
        """ % (CHECKM_DB, CHECKM_DB)

rule checkm_db:
    input:
        tar="temp.%s.tar.gz" % CHECKM_DB
    params:
        db=CHECKM_DB
    output:
        CHECKM_DB+"/.dmanifest"
    shell:
        """
        mkdir -p {params.db}
        tar -xvf {input.tar} -C {params.db}
        """
