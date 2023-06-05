include: "../../../setup_rules/pigz.smk"
CONTAINER = "virsorter.sif"

rule singularity:
    input:
        "pigz",
        CONTAINER,
        "virsorter_db"

rule virsorter:
    output:
        CONTAINER
    shell:
        """
        singularity pull {output} docker://staphb/virsorter2
        """

rule virsorter_db:
    input:
        CONTAINER
    output:
        "virsorter_db"
    shell:
        """
        singularity run -B ./:/ws {input} virsorter setup --db-dir /ws/{output}
        """
