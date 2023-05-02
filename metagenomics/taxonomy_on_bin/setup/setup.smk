VERSION = "207_v2" # check url in download, if changing version!
CONTAINER = "gtdbtk.sif"
GTDBTK_REF_DL = f"gtdbtk_r{VERSION}_data.tar.gz"

rule singularity:
    input:
        CONTAINER,
        GTDBTK_REF

rule get_image:
    output:
        CONTAINER
    shell:
        """
        singularity build {output} docker://ecogenomic/gtdbtk:2.2.6
        """

# check url if change version
rule download_gtdbtk_refdata:
    output:
        GTDBTK_REF_DL
    shell:
        """
        wget --quiet https://data.gtdb.ecogenomic.org/releases/release207/207.0/auxillary_files/{output}
        """

# rule extract_gtdbtk_refdata:
#     input:
#         GTDBTK_REF_DL
#     output:
#         GTDBTK_REF
#     params:
#         folder=GTDBTK_REF_FOLDER,
#         release=f"release{VERSION}"
#     shell:
#         """
#         tar -xf {input} -C {params.folder} \
#         && cd {params.folder} \
#         && mv {params.release}/* ./
#         && rmdir {params.release}
#         """
