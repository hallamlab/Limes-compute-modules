include: "../../../setup_rules/diamond.smk"
include: "../../../setup_rules/pigz.smk"

COG = "cog-20"
# remember to change the link in get_cog

rule singularity:
    input:
        "diamond.sif",
        "%s.dmnd"%COG
    shell:
        """
        [[ -f %s.faa ]] && rm %s.faa
        [[ -f %s.fa.gz ]] && rm %s.fa.gz
        echo 0
        """%((COG,)*4)

rule get_cog:
    output:
        "%s.fa.gz"%COG
    shell:
        """
        wget -q https://ftp.ncbi.nih.gov/pub/COG/COG2020/data/cog-20.fa.gz
        """
