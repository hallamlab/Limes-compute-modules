rule python_for_data_science:
    output:
        "python_for_data_science.sif"
    shell:
        """
        singularity pull {output} docker://quay.io/hallamlab/python_for_data_science
        """
