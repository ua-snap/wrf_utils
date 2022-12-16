"""Get date and time information and the forecast_time attribute for each hourly WRF file in a given directory.
This script has been adapted to allow creation of the table from files on the $ARCHIVE filesystem, in case all of the data for a particular WRF group will not fit on the $CENTER1 (scratch) filesystem.
"""

import argparse
import time
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
import xarray as xr
# project
from config import *


def get_forecast_time(fp):
    """Get the forecast time attribute for the file
    
    Args:
        fp (path_like): path to hourly WRF file to get
            forecast_time attribute from
        
    Returns:
        forecast_time (int): value of file's forecast time attribute
    """
    with xr.open_dataset(fp) as ds:
        forecast_time = ds["PCPT"].attrs["forecast_time"]

    return forecast_time


def get_date_info(fp):
    """Get the date information a single raw WRF filepath
    
    Args:
        fp (pathlib.PosixPath): path to raw hourly WRF file
    
    Returns:
        date_info (dict): dict of relevant date information
    """
    year, month, day_hour = fp.name.split(".")[-2].split("-")
    day, hour = day_hour.split("_")
    folder_year = fp.parent.name
    date_info = {
        "filepath": fp,
        "year": year,
        "folder_year": folder_year,
        "month": month,
        "day": day,
        "hour": hour,
    }

    return date_info


def list_files(dirpath):
    """list the files and split the filenames into their descriptor parts and return dataframe of elements and filename sorted by:['year', 'month', 'day', 'hour']
    
    Args:
        dirpath (pathlib.PosixPath): path to the directory containing annual subdirs of hourly WRF outputs
    
    Returns:
        files_df (pandas.DataFrame): dataframe of info derived from all files in dirpath
    """
    # using 'WRFDS' prefix to match standard raw outputs
    files = [get_date_info(fp) for fp in list(dirpath.glob("*/WRFDS*.nc"))]
    files_df = pd.DataFrame(files)
    files_df = files_df.sort_values(["year", "month", "day", "hour"]).reset_index()

    return files_df


def get_file_attrs(fp):
    """Get file file date and time info and the "forecast_time" attribute from hourly WRF ouput file
    
    Args:
        fp (path_like): hourly WRF filepath to get attribute and info for
        
    Returns:
        fp_args (dict): dict of file's date info and forecast_time attribute
    """
    try:
        fp_args = get_date_info(fp)
        fp_args.update({"forecast_time": get_forecast_time(fp)})
    except:
        # if there is an issue... dont fail, do this...
        nodata = -9999
        fp_args = {
            "filepath": fp,
            "year": nodata,
            "folder_year": nodata,
            "month": nodata,
            "day": nodata,
            "hour": nodata,
            "forecast_time": nodata,
        }

    return fp_args


def replace_archive_prefix(df, prefix):
    """In the case that source WRF folder is on $ARCHIVE, replace the filepaths in the output file with what they should be on scratch space
    
    Args:
        df (pandas.DataFrame): Time series attribute table being created
        prefix (pathlib.PosixPath): path to hourly directory containing annual subdirs on scratch space
    
    Returns:
        df (pandas.DataFrame): Update time series attribute table
    """
    def replace(fp, prefix):
        # join year then filename
        return prefix.joinpath(fp.parent.name, fp.name)
    
    df["filepath"] = df["filepath"].apply(lambda x: replace(x, prefix))
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get forecast times and date info from hourly WRF files in parallel"
    )
    parser.add_argument(
        "-s",
        "--wrf_dir",
        action="store",
        dest="wrf_dir",
        type=str,
        default="",
        help=(
            "path to the directory in scratch_dir containing the hourly"
            "WRF output files to get forecast_time attribute from"
        ),
    )
    # This switch is probably only going to be used when there is not enough space on the scratch filesystem to copy everything off of $ARCHIVE
    parser.add_argument(
        "--is_archive",
        action="store_true",
        dest="is_archive",
        default=False,
        help="Switch for wrf_dir being on the $ARCHIVE filesystem",
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        type=int,
        default=1,
        help="Number of CPUs to use for parallel reading of files",
    )
    cl_args = parser.parse_args()
    wrf_dir = Path(cl_args.wrf_dir)
    ncpus = cl_args.ncpus

    fp_df = list_files(wrf_dir)
    fp_df = fp_df[fp_df.folder_year == fp_df.year]
    print(f"number of files: {len(fp_df)}")

    args = [df["filepath"] for group, df in fp_df.groupby("year")]

    tic = time.perf_counter()
    out = []
    for arg in args:
        with Pool(ncpus) as pool:
            out.extend(pool.map(get_file_attrs, arg))
    print(f"Pooling done, time elapsed: {round(time.perf_counter() - tic)}s")

    df = pd.DataFrame(out)
    
    # if wrf_dir is in $ARCHIVE, we want to replace all of the file paths with what the path SHOULD be on scratch space
    if cl_args.is_archive:
        replace_archive_prefix(df, hourly_dir.joinpath(group))
    
    ftime_fp = anc_dir.joinpath(f"WRFDS_forecast_time_attr_{group}.csv")
    df.to_csv(ftime_fp, index=False)
    print(f"Forecast times table for {hourly_dir} written to {ftime_fp}")
