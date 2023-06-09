include: "../../../setup_rules/diamond.smk"
include: "../../../setup_rules/python_for_data_science.smk"

MOBILEOG_DB="mobileogdb_latest"

rule singularity:
    input:
        img="diamond.sif",
        sci="python_for_data_science.sif",
        db=f"%s.dmnd"%MOBILEOG_DB,
        kb=f"%s.csv"%MOBILEOG_DB
    params:
        raw_dl="%s.zip"%MOBILEOG_DB
    shell:
        """
        [[ -f %s.zip ]] && rm %s.zip
        [[ -f %s.faa ]] && rm %s.faa
        echo 0
        """%((MOBILEOG_DB,)*4)

rule get_mobileog:
    output:
        "%s.zip"%MOBILEOG_DB
    shell:
        """
        link=$(wget -q https://mobileogdb.flsi.cloud.vt.edu/entries/database_download -O - \
            | grep "Download All Data" | head -n 1 \
            | cut -c 73- | rev | cut -c 24- | rev \
            | sed --expression='s/amp;//g')
        wget -q "$link" -O {output}
        """

rule extract_get_mobileog:
    input:
        "%s.zip"%MOBILEOG_DB
    output:
        kb="%s.csv"%MOBILEOG_DB,
        db="%s.faa"%MOBILEOG_DB
    shell:
        """
        mkdir %s.temp
        cd %s.temp
        unzip ../{input}
        rm -r __MACOSX *README.txt
        mv *.csv ../{output.kb}
        mv *.faa ../{output.db}
        cd ../
        rm -r %s.temp
        """%((MOBILEOG_DB,)*3)
