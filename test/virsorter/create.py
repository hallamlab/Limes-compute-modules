import os, sys
sys.path = list(set([
    "../../lib/"
]+sys.path))
from pathlib import Path
import limes_x as lx

lx.ModuleBuilder.GenerateTemplate(
    modules_folder="/home/tony/workspace/python/Limes-all/Limes-compute-modules/metagenomics",
    name="find_viruses",
    on_exist="skip",
)
