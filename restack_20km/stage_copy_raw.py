"""Stage hourly WRF outputs for WRF group set in WRF_GROUP env var. 

Instead of doing a single batch_stage for the entire WRF group directory, this script will iterate over all years available for a single WRF group and stage the yearly directories.

Process:
  1. initiate the batch stage
  2. Once batch_stage completes, run the rsync to copy
"""

import argparse
import time
import os
# from subprocess import check_output
import subprocess
# project imports
from config import *
import luts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-y", dest="year", type=int, help="Single year to stage and copy")
    args = parser.parse_args()
    year = args.year
    
    wrf_dir = Path(luts.groups[group]["directory"])
    dst_dir = raw_scratch_dir.joinpath(group)
    
    if year is None:
        years = luts.groups[group]["years"]
    else:
        years = [year]

    # run the srage and rsync for all years
    tic = time.perf_counter()
    for year in years:
        dst_year_dir = dst_dir.joinpath(str(year))
        dst_year_dir.mkdir(exist_ok=True)
        if len(list(dst_year_dir.glob("WRFDS*.nc"))) > 8700:
            # cheap way to avoid re-staging directories that are (probably) already copied
            print(f"{dst_year_dir.name} already done, skipping")
            continue
	# cannot stage the directories in general because there could be other irrelevant large files.
        #  Stage the netCDFs only.
        stage_dir = wrf_dir.joinpath(str(year))
        os.chdir(stage_dir)
        print(f"Staging {stage_dir}")
        tic2 = time.perf_counter()
        
        # testing different method for staging all netcdf files
        ls_proc = subprocess.Popen(["ls", "WRFDS*.nc"], stdout=subprocess.PIPE, shell=True)
        stage_proc = subprocess.Popen(["batch_stage", "-i"], stdin=ls_proc.stdout)
        # try communicating output before moving on
        _ = stage_proc.communicate()

        print(f"done, files staged in {round((time.perf_counter() - tic2) / 60)}m")
        # run rsync
        print(f"running rsync on {stage_dir}")
        tic2 = time.perf_counter()
        dst_year_dir = dst_dir.joinpath(str(year))
        dst_year_dir.mkdir(exist_ok=True)
        _ = os.system(f"rsync -a WRFDS*.nc {dst_year_dir}")
        print(f"rsync completed in {round((time.perf_counter() - tic2) / 60)}m")

    print(f"Files staged and copied in {round((time.perf_counter() - tic) / 60)}m")
