PIGZ = "pigz"

rule get_pigz:
    output:
        "pigz.tar.gz"
    shell:
        """
        wget https://zlib.net/pigz/{output}
        """

rule compile_pigz:
    output:
        PIGZ
    input:
        "pigz.tar.gz"
    shell:
        """
        tar -xf {input} && mv pigz pigz_lib \
        && cd pigz_lib && make \
        && chmod +x pigz && cp pigz ../ && cd ../ && rm -rf pigz_lib \
        && rm {input}
        """

rule pigz_unzip:
    input:
        zipped="{file}.gz",
        pigz=PIGZ
    output:
        "{file}"
    threads: 2
    shell:
        """
        {input.pigz} -dc -k -p {threads} {input.zipped} >{output}
        """

# rule pigz_untar:
#     input:
#         zipped="{file}.tar.gz",
#         pigz=PIGZ
#     output:
#         "{file}"
#     shell:
#         """
#         {input.pigz} -dc -k {input.zipped} | tar xf -
#         """
