#!/cvmfs/soft.computecanada.ca/easybuild/software/2020/avx2/Core/python/3.11.2/bin/python
#########################################################################################
# hpc submit 

NAME    = "fqc03b"
CPU     = 2
MEM     = 16
TIME    = "2:00:00"
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

    _cy = Path("/home/phyberos/project-rpp/cyanocyc")
    dest_dir = _cy.joinpath("reads.stats")
    haves = set(os.listdir(dest_dir))

    with open("../logistics/srr_reads.json") as j:
        srr_list = [s for s in json.load(j) if s not in haves]

    _root = Path("/home/phyberos/project-rpp/cyanocyc/reads")
    # sample_list = list(os.listdir(_root))
    sample_list = srr_list
    for i, srr in enumerate(sample_list):
        if srr not in srr_list: continue
        print(f"\rpreparing {i+1} of {len(sample_list)} | {srr}", end="")
        rpath = _root.joinpath(srr)
        context.append(dict(
            srr=str(srr),
            reads=[str(r) for r in os.listdir(rpath)],
            rpath=str(rpath),
        ))
    print()
    print(f"found {len(context)}")

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
reads = [r for r in DATA["reads"]] # relative, (only file names)
rpath = Path(DATA["rpath"])

print(f"getting inputs {srr}")
out=OUT_DIR.joinpath(srr)
os.makedirs(out, exist_ok=True)

print("get reqs")
os.system(f"""\
    mkdir -p reads
    cp -r {rpath}/* ./reads
    cp /home/phyberos/project-rpp/scripts/lock_step/m/lib/fastqc.sif ./
    mkdir -p out
""")

print("start")
os.system(f"""\
    singularity exec -B ./:/ws ./fastqc.sif \
        fastqc --noextract -o /ws/out {" ".join([f"/ws/reads/{r}" for r in reads])}
""")

print("cp back")
os.system(f"""\
    cp ./out/* {out}
""")

# ---------------------------------------------------------------------------------------
# done
print('done')
