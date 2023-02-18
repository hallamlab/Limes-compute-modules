VERSION = "207_v2" # check url in download, if changing version!
CONTAINER = "gtdbtk.sif"
GTDBTK_REF_FOLDER = "gtdbtk_data"
GTDBTK_REF = f"{GTDBTK_REF_FOLDER}/taxonomy/gtdb_taxonomy.tsv"
GTDBTK_REF_DL = f"temp.{GTDBTK_REF}.tar.gz"

rule singularity:
    input:
        CONTAINER,
        GTDBTK_REF,

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity pull {output} library://edwardbirdlab/bara/gtdbtk_2
        """

# check url if change version
rule download_gtdbtk_refdata:
    output:
        GTDBTK_REF_DL
    params:
        original_name=f"gtdbtk_r{VERSION}_data.tar.gz"
    shell:
        """
        wget --quiet https://data.gtdb.ecogenomic.org/releases/release207/207.0/auxillary_files/{params.original_name}
        mv {params.original_name} {output}
        """

rule extract_gtdbtk_refdata:
    input:
        GTDBTK_REF_DL
    output:
        GTDBTK_REF
    params:
        folder=GTDBTK_REF_FOLDER
        release=f"release{VERSION}"
    shell:
        """
        tar -xvf {input} -C {params.folder} \
        && cd {params.folder} \
        && mv {params.release}/* ./
        && rmdir {params.release}
        """ % VERSION
