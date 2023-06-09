include: "../../../setup_rules/pigz.smk"
include: "../../../setup_rules/diamond.smk"
include: "../../../setup_rules/python_for_data_science.smk"

rule singularity:
    input:
        img="diamond.sif",
        sci="python_for_data_science.sif",
        db="eggnog_proteins.dmnd",
        kb="eggnog.db"
    shell:
        """
        [[ -f {input.kb}.gz ]] && rm {input.kb}.gz
        [[ -f {input.db}.gz ]] && rm {input.db}.gz
        echo 0
        """

rule get_eg_proteins:
    output:
        "eggnog_proteins.dmnd.gz"
    shell:
        """
        wget -q http://eggnog5.embl.de/download/emapperdb-5.0.2/eggnog_proteins.dmnd.gz
        """

rule get_eg_kb:
    output:
        "eggnog.db.gz"
    shell:
        """
        wget -q http://eggnog5.embl.de/download/emapperdb-5.0.2/eggnog.db.gz
        """
