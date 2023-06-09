include: "../../../setup_rules/diamond.smk"
include: "../../../setup_rules/pigz.smk"

METACYC = "metacyc-2020-08-10"

rule singularity:
    input:
        img="diamond.sif",
        db="%s.dmnd"%METACYC
    shell:
        """
        [[ -f %s.faa ]] && rm %s.faa
        echo 0
        """%((METACYC,)*2)

rule get_metacyc:
    output:
        "%s.faa"%METACYC
    shell:
        """
        wget -q https://ndownloader.figshare.com/files/27419069 -O {output}
        """

