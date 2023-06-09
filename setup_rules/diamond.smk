rule diamond:
    output:
        "diamond.sif"
    shell:
        """
        singularity pull {output} docker://bschiffthaler/diamond:2.0.14
        """

rule format_diamond_db:
    input:
        db="{db}.faa",
        img="diamond.sif"
    output:
        "{db}.dmnd"
    threads: 16
    shell:
        """
        singularity run -B ./:/ws {input.img} makedb --in {input.db} -p {threads} -d {output}
        """
