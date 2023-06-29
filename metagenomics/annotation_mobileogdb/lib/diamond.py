from pathlib import Path
from typing import Callable
from limes_x import ModuleBuilder, Item, JobContext, JobResult

def RunDiamond(orfs: Path, db: Path, out_dir: Path, diamond_sif: Path, shell: Callable, threads: int, mem_gb: int):
    name = orfs.name.replace(".orfs.", ".")
    name = ".".join(name.split(".")[:-1])

    orf_dir, orf_file = orfs.parent, orfs.name
    db_dir, db_file = db.parent, db.name
    db_name = ".".join(db_file.split(".")[:-1])
    db_name = db_name.replace(".", "_")

    binds = [
        f"{orf_dir}:/dmnd_in",
        f"{db_dir}:/dmnd_db",
        f"{out_dir}:/dmnd_out",
    ]

    key = f"{name}.{db_name}"
    table_file = f"{key}.hits.tsv"
    columns = f"qseqid stitle qstart qend nident pident evalue".split(" ")
    # --very-sensitive is nearly as accurate as blastp but 300x faster
    # https://bmcgenomics.biomedcentral.com/articles/10.1186/s12864-020-07132-6
    # https://www.nature.com/articles/s41592-021-01101-x/figures/1
    run_result = shell(f"""\
        singularity run -B {",".join(binds)} {diamond_sif} \
            blastp --very-sensitive --threads {threads} --memory-limit {mem_gb} \
            --outfmt 6 {" ".join(columns)} \
            --db /dmnd_db/{db_file} \
            --query /dmnd_in/{orf_file} \
            --out /dmnd_out/{table_file}
    """)

    return run_result, key, out_dir.joinpath(table_file), columns
