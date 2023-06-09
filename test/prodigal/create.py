import os, sys
sys.path = list(set([
    "../../lib/"
]+sys.path))
from pathlib import Path
import limes_x as lx

lx.ModuleBuilder.GenerateTemplate(
    modules_folder="../../metagenomics",
    name="orf_prediction",
    on_exist="skip",
)
