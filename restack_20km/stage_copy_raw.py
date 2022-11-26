"""Stage hourly WRF outputs for WRF group set in WRF_GROUP env var. 

Instead of doing a single batch_stage for the entire WRF group directory, this script will iterate over all years available for a single WRF group and stage the yearly directories.

Process:
  1. initiate the batch stage
  2. Once batch_stage completes, run the rsync to copy
"""

import time
import os
from subprocess import check_output
# project imports
from config import *
import luts


if __name__ == "__main__":
    years = luts.groups[group]["years"]
    wrf_dir = Path(luts.groups[group]["directory"])
    dst_dir = raw_scratch_dir.joinpath(group)

    tic = time.perf_counter()
    # run the srage and rsync for all years
    for year in years:
        # cannot stage the directories in general because there could be other irrelevant large files.
        #  Stage the netCDFs only.
        stage_dir = wrf_dir.joinpath(str(year))
        os.chdir(stage_dir)
        print(f"Staging {stage_dir}")
        tic2 = time.perf_counter()
        # using check output to avoid printing all staging processes (one line for each file)
        out = check_output(["ls WRFDS*.nc", "|", "batch_stage", "-i"])
        print(f"done, files staged in {round((time.perf_counter() - tic2) / 60)}m")
        # run rsync
        print(f"running rsync on {stage_dir}")
        tic2 = time.perf_counter()
        dst_year_dir = dst_dir.joinpath(year)
        dst_year_dir.mkdir(exist_ok=True)
        _ = os.system(f"rsync -a WRFDS*.nc {dst_year_dir}")
        print(f"rsync completed in {round((time.perf_counter() - tic2) / 60)}m")
    
    print(f"Files staged and copied in {round((time.perf_counter() - tic) / 60)}m")
