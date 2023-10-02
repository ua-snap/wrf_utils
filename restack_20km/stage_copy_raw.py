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
    parser.add_argument("-y", dest="year", type=str, help="A ' '-separated list of years, such as '2033 2064'")
    parser.add_argument("-r", dest="year_range", type=str, help="Range of years to stage and copy")
    parser.add_argument("-s", dest="stage", action="store_true", help="Attempt to stage the directories before copying")
    args = parser.parse_args()
    year = args.year
    year_range = args.year_range
    stage = args.stage
    
    wrf_dir = Path(luts.groups[group]["directory"])
    dst_dir = raw_scratch_dir.joinpath(group)
    
    if year is not None:
        years = [int(year) for year in year.split(" ")]
    elif year_range is not None:
        start_year, end_year = year_range.split("-")
        years = list(range(int(start_year), int(end_year) + 1))
    else:
        years = luts.groups[group]["years"]

    # run the srage and rsync for all years
    tic = time.perf_counter()
    for year in years:
        dst_year_dir = dst_dir.joinpath(str(year))
        dst_year_dir.mkdir(exist_ok=True, parents=True)
        if len(list(dst_year_dir.glob("WRFDS*.nc"))) > 8700:
            # cheap way to avoid re-staging directories that are (probably) already copied
            # not looking for exact number of expected files for most years because some years are shorter :/
            print(f"{dst_year_dir.name} already done, skipping")
            continue
            
        stage_dir = wrf_dir.joinpath(str(year))
        os.chdir(stage_dir)
        if stage:
            # cannot stage the directories in general because there could be other irrelevant large files.
            #  Stage the netCDFs only.
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
        _ = os.system(f"rsync -a WRFDS*.nc {dst_year_dir}")
        print(f"rsync completed in {round((time.perf_counter() - tic2) / 60)}m")

    print(f"Files staged and copied in {round((time.perf_counter() - tic) / 60)}m")
