VERSION = "207_v2" # check url in download, if changing version!
CONTAINER = "checkm.sif"

CHECKM_DB = "checkm_data_2015_01_16"
CHECKM_DB_REP = CHECKM_DB+"/.dmanifest"

rule singularity:
    input:
        CONTAINER,
        CHECKM_DB_REP,

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity build {output} docker://nanozoo/checkm
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
        CHECKM_DB_REP
    shell:
        """
        mkdir -p {params.db}
        tar -xvf {input.tar} -C {params.db}
        """
