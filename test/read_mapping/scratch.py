#!/cvmfs/soft.computecanada.ca/easybuild/software/2020/avx2/Core/python/3.11.2/bin/python

import os, sys, stat
from pathlib import Path
import json

cpus = 2
WS = Path("/home/phyberos/scratch/test/ws")
os.mkdir(WS)
OUT_DIR = Path("/home/phyberos/scratch/test/out")
os.mkdir(OUT_DIR)

DATA = {
    "srr": "SRR24300479",
    "asm": "/home/phyberos/project-rpp/cyanocyc/asms/SRR24300479.asm.fa",
    "reads": "/home/phyberos/project-rpp/cyanocyc/reads/SRR24300479",
}

srr = DATA["srr"]
asm_path = Path(DATA["asm"])
reads_path = Path(DATA["reads"])

READS = Path("./reads")
ASM = Path(f"./{srr}.fa")
os.system(f"""\
    mkdir -p {READS}
    cp /home/phyberos/project-rpp/scripts/m_genomeQC/read_alignment.sif ./
    cp /home/phyberos/project-rpp/scripts/m_spades/quast.sif ./
    cp {asm_path} {ASM}
    cp -r {reads_path}/* {READS}/
    find .
""")

READ_FILES = [READS.joinpath(f) for f in os.listdir(READS)]

#https://bioinformatics.stackexchange.com/questions/935/fast-way-to-count-number-of-reads-and-number-of-bases-in-a-fastq-file
read_sizes = WS.joinpath("temp.readcount.txt")
os.system(f"""\
    pigz -p {cpus} -dc {READ_FILES[0]} \
    | awk 'NR % 4 == 2' \
    | wc -cl >{read_sizes} \
""")

LONG_READ_THRESHOLD = 1000
is_short_read = True
with open(read_sizes) as f:
    toks = f.readline()[:-1].strip()
    if "\t" in toks: toks = toks.split("\t")
    else: toks = [t for t in toks.split(" ") if len(t)>0]
    num_reads, nucleotides = toks
    av_len = int(nucleotides)/int(num_reads)
    print(f"average read length: {av_len}")
    if av_len > LONG_READ_THRESHOLD:
        is_short_read = False
sr_params = f"-x sr" if is_short_read else ""

if is_short_read and len(READ_FILES) > 2:
    _pe_reads = ' '.join([str(f) for f in READ_FILES if "_1" in f.name or "_2" in f.name])
    _se = ' '.join([str(f) for f in READ_FILES if not ("_1" in f.name or "_2" in f.name)])
    align = f"""\
        minimap2 -a {sr_params} --secondary=no {ASM} {_pe_reads} | samtools view -b -o temp.pe -
        minimap2 -a {sr_params} --secondary=no {ASM} {_se} | samtools view -b -o temp.se -
        samtools merge $BAM --write-index temp.pe temp.se
    """
else:
    align = f"""\
        minimap2 -a {sr_params} --secondary=no {ASM} {' '.join([str(f) for f in READ_FILES])} | samtools sort -o $BAM --write-index -
    """

if is_short_read:
    get_unmapped = f"""\
        samtools view -u  -f 4 -F 8 $BAM  > unmapped1.bam   # single unaligned
        samtools view -u  -f 8 -F 4 $BAM  > unmapped2.bam   # other unaligned
        samtools view -u  -f 12 $BAM > unmapped3.bam        # both
        samtools merge -u - unmapped[123].bam | samtools sort -n - -o unmapped.bam
        bamToFastq -i unmapped.bam -fq /out/{srr}_unmapped_1.fq -fq2 /out/{srr}_unmapped_2.fq 2>/dev/null
    """
else:
    get_unmapped = f"""\
        samtools view -u  -f 4 $BAM  > unmapped.bam         # single unaligned
        bamToFastq -i unmapped.bam -fq /out/{srr}_unmapped.fq 2>/dev/null
    """

bounce = "bounce.sh"
with open(WS.joinpath(bounce), "w") as f:
    f.write(f"""\
        cd /ws
        BAM=./temp.coverage.bam

        {align}
        
        {get_unmapped}

        bedtools genomecov -ibam $BAM -bg >./{srr}.coverage.tsv
        cp {srr}.coverage.tsv /out/
        samtools flagstat $BAM >/out/{srr}.stats.txt
        echo "base pairs: {nucleotides}" >>/out/{srr}.stats.txt
    """)
os.chmod(WS.joinpath(bounce), stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
out = OUT_DIR.joinpath(srr)
os.system(f"""\
    cat {bounce}
    mkdir -p {out}
    singularity run -B {WS}:/ws,{out}:/out {WS}/read_alignment.sif \
        /ws/{bounce}

    singularity run -B {WS}/:/ws,{out}:/out {WS}/quast.sif \
    quast -t {cpus} \
        -o /ws/quast \
        /ws/{ASM}

    cp ./quast/transposed_report.tsv {out}/{srr}.quast.tsv
    tar -cf - ./quast | pigz -7 -p {cpus} >{out}/{srr}.quast.tar.gz
""")
