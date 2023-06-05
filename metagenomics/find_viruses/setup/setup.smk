include: "../../../setup_rules/pigz.smk"
CONTAINER = "virsorter.sif"

rule singularity:
    input:
        "pigz",
        CONTAINER,
        "virsorter_db/Done_all_setup"

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
        "virsorter_db/Done_all_setup"
    shell:
        """
        mkdir -p TEMP.virsorter_src
        singularity run -B ./:/ws virsorter2.sif cp -r /VirSorter2/virsorter /ws/TEMP.virsorter_src
        singularity run -B ./:/ws,./TEMP.virsorter_src:/VirSorter2/virsorter {input} virsorter setup --db-dir /ws/{output}
        rm -r TEMP.virsorter_src
        """
