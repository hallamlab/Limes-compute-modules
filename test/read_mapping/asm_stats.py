#!/cvmfs/soft.computecanada.ca/easybuild/software/2020/avx2/Core/python/3.11.2/bin/python
#########################################################################################
# hpc submit 

NAME    = "asm_stats05b"
CPU     = 2
MEM     = 8
TIME    = "8:00:00"
# USER    = "txyliu"            # pbs (sockeye)
# ALLOC   = "st-shallam-1"      # pbs (sockeye)
# TEMP_VAR= "TMPDIR"            # pbs (sockeye)
# EMAIL   = "txyliu@student.ubc.ca"
USER    = "phyberos"            # slurm (cedar)
ALLOC   = "rpp-shallam"         # slurm (cedar)
TEMP_VAR= "SLURM_TMPDIR"        # slurm (cedar)

# if this script is called directly, then submit witn _INNER flag
# if _INNER is in the arguments, then I'm running on a compute node
# so continue with workflow
import os, sys, stat
import json
import time
import uuid
from pathlib import Path
from datetime import datetime
_INNER = "inner"
SCRIPT = os.path.abspath(__file__)
SCRIPT_DIR = Path("/".join(SCRIPT.split("/")[:-1]))

if not (len(sys.argv)>1 and sys.argv[1] == _INNER):
    now = datetime.now() 
    date_time = now.strftime("%Y-%m-%d-%H-%M")
    run_id = f'{uuid.uuid4().hex[:3]}'
    print("run_id:", run_id)
    OUT_DIR = Path(f"/home/{USER}/scratch/runs/{run_id}.{NAME}.{date_time}")
    internals_folder = OUT_DIR.joinpath(f"internals")

    context = []
    # ---------------------------------------------------------------------------------
    # get data for run

    # with open("../logistics/failed_asm_stats.json") as j:
    #     srr_list = json.load(j)

    _cy = Path("/home/phyberos/project-rpp/cyanocyc")
    _asms = Path("/home/phyberos/project-rpp/cyanocyc/asms")
    _reads = Path("/home/phyberos/project-rpp/cyanocyc/reads")
    dest_dir = _cy.joinpath("asms.stats")
    haves = set(os.listdir(dest_dir))

    # print()
    # print(f"redo {len(redo)}")

    # with open("../logistics/srr_asms.json") as j:
    #     srr_list = [s for s in json.load(j) if s in redo or s not in haves]

    with open("./redo_asm_stats.json") as j:
        srr_list = json.load(j)

    # srr_list = ["ERR1713486", "ERR1713489", "SRR18114235"]
    # sample_list = list(os.listdir(_root))
    for i, srr in enumerate(srr_list):
        # srr = asm.split(".")[0]
        asm = f"{srr}.asm.fa"
        asm_path = _asms.joinpath(asm)
        if not asm_path.exists(): continue
        context.append(dict(
            srr=str(srr),
            asm=str(asm_path),
            reads=str(_reads.joinpath(srr)),
        ))

    print(f"N: {len(context)}")

    # ---------------------------------------------------------------------------------
    # prep commands & workspaces

    if len(context) == 0:
        print(f"no jobs, stopping")
        exit()

    os.makedirs(internals_folder)
    os.chdir(internals_folder)

    run_context_path = internals_folder.joinpath("context.json")
    with open(run_context_path, "w") as j:
        json.dump(context, j, indent=4)

    os.makedirs(OUT_DIR, exist_ok=True)

    # ---------------------------------------------------------------------------------
    # submit

    notes_file = f"notes.{run_id}.txt"
    run_cmd = f"{SCRIPT} {_INNER} {run_context_path} {OUT_DIR} {CPU} {MEM} {TIME} {run_id}"
    #########
    # # pbs
    # pbs_bounce = internals_folder.joinpath(f"{NAME}.{run_id}.sh")
    # with open(pbs_bounce, 'w') as s:
    #     s.write(f"""\
    #         PYTHONPATH=""
    #         {run_cmd}
    #     """)
    # os.chmod(pbs_bounce, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
    # time.sleep(1)
    # arr_param = f"-J 0-{len(context)-1}" if len(context)>1 else " "
    # sub_cmd = f"""qsub \
    #     -A {ALLOC} \
    #     -N "{run_id}-{NAME}" \
    #     -o {internals_folder}/^array_index^.out \
    #     -e {internals_folder}/^array_index^.err \
    #     -M {EMAIL} -m n \
    #     -l "walltime={TIME},select=1:ncpus={CPU}:mem={MEM}gb" \
    #     {arr_param} \
    #     {pbs_bounce} >>{internals_folder}/{notes_file}
    # """.replace("  ", "")
    #########
    # slurm
    arr_param = f"--array=0-{len(context)-1}" if len(context)>1 else " "
    sub_cmd = f"""\
    sbatch --job-name "{run_id}-{NAME}" \
        --account {ALLOC} \
        --error {internals_folder}/err.%a.log --output {internals_folder}/out.%a.log \
        --cpus-per-task={CPU} --mem={MEM}G --time={TIME} \
        {arr_param} \
        --wrap="{run_cmd}" &>> {internals_folder}/{notes_file}
    ln -s {internals_folder} {SCRIPT_DIR}/{NAME}.{date_time}.{run_id}
    """.replace("  ", "")
    #########
    with open(notes_file, "w") as log:
        log.writelines(l+"\n" for l in [
            f"name: {NAME}",
            f"id: {run_id}",
            f"array size: {len(context)}",
            f"output folder: {OUT_DIR}",
            f"submit command:",
            sub_cmd,
            "",
        ])
    if not (len(sys.argv)>1 and sys.argv[1] in ["--mock", "mock", "-m"]):
        os.chdir(OUT_DIR)
        os.system(sub_cmd)
        print("submitted")

    exit() # the outer script

#########################################################################################
# on the compute node...

_, run_context_path, _out_dir, cpus, mem, given_time, run_id = sys.argv[1:] # first is just path to this script
setup_errs = []
arr_var = "SLURM_ARRAY_TASK_ID"
# arr_var = "PBS_ARRAY_INDEX"
if arr_var in os.environ:
    job_i = int(os.environ[arr_var])
else:
    _e = f'echo "not in array, defaulting to the first context"'
    setup_errs.append(_e)
    os.system(_e)
    job_i = 0
with open(run_context_path) as f:
    run_context = json.load(f)
assert run_context is not None
DATA = run_context[job_i]
OUT_DIR = Path(_out_dir)

def print(x):
    now = datetime.now() 
    date_time = now.strftime("%H:%M:%S")
    with open(OUT_DIR.joinpath(f"{run_id}.{job_i}.log"), "a") as log:
        log.write(f"{date_time}> {x}\n")
for _e in setup_errs:
    print(_e)
print(f"job:{job_i+1}/{len(run_context)} cpu:{cpus} mem:{mem} time:{given_time} ver:4")
print("-"*50)

# ---------------------------------------------------------------------------------------
# setup workspace in local scratch

salt = uuid.uuid4().hex
WS = Path(os.environ.get(TEMP_VAR, '/tmp')).joinpath(f"{NAME}-{salt}"); os.makedirs(WS)
os.chdir(WS)

# ---------------------------------------------------------------------------------------
# work

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
        minimap2 -a --secondary=no {ASM} {_se} | samtools view -b -o temp.se -
        samtools merge - --write-index temp.pe temp.se | samtools sort --threads {cpus} -o $BAM --write-index -
    """
else:
    align = f"""\
        minimap2 -a {sr_params} --secondary=no {ASM} {' '.join([str(f) for f in READ_FILES])} | samtools sort --threads {cpus} -o $BAM --write-index -
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

# ---------------------------------------------------------------------------------------
# done
print('done')
