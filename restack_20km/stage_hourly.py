"""Stage hourly WRF outputs for WRF group set in WRF_GROUP env var. 

Instead of doing a single batch_stage for the entire WRF group directory, this script will iterate over all years available for a single WRF group and stage the yearly directories.
"""

import time
from subprocess import check_output
# project imports
from config import *
import luts


if __name__ == "__main__":
    years = luts.groups[group]["years"]
    wrf_dir = luts.groups[group]["directory"]

    tic = time.perf_counter()
    stdout = []
    for year in years:
        stage_dir = wrf_dir.joinpath(str(year))
        out = check_output(["batch_stage", "-r", stage_dir])
        stdout.append(out)
        
    print(f"Files staged in {round((time.perf_counter() - tic) / 60)}m")
