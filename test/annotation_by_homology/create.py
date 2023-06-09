import os, sys
sys.path = list(set([
    "../../lib/"
]+sys.path))
from pathlib import Path
import limes_x as lx

names = [f"annotation_{n}" for n in [
    "eggnog",
    "cog",
    "metacyc",
    "mobileogdb",
]]

for n in names:
    lx.ModuleBuilder.GenerateTemplate(
        modules_folder="../../metagenomics",
        name=n,
        on_exist="skip",
    )
