"""Resaple hourly file to daily. Reads a file, resamples based on provided aggregation info, writes new daily file.

Usage:
    Designed to be called via slurm scripts created by `slurm.write_sbatch_resample.py`
"""

import argparse
import time
from multiprocessing import Pool
from pathlib import Path
import xarray as xr
import numpy as np
import pandas as pd


def resample(fp, aggr, varname, out_varname, out_fp):
    """Resample a restacked hourly WRF dataset to a daily resolution using the provided aggregation function
    
    Args:
        fp (path_like): path to hourly WRF dataset
        aggr (str): aggregation function to use
        varname (str): name of WRF variable
        out_varname (str): name of the new aggregate variable
        out_fp (pathlike): path to write resampled dataset
        
    Returns:
        day_ds (xarray.Dataset): daily WRF dataset created from hourly input
    """
    # do the resampling based on aggregation type
    with xr.open_dataset(fp) as ds:
        # metric switch -- aggregation
        aggr_str = "1D"
        if aggr == "mean": 
            ds_day = ds.resample(time=aggr_str).mean()
        elif aggr == "min": 
            ds_day = ds.resample(time=aggr_str).min()
        elif aggr == "max": 
            ds_day = ds.resample(time=aggr_str).max()
        elif aggr == "sum": 
            ds_day = ds.resample(time=aggr_str).sum()

    ds_day = ds_day.rename({wrf_varname: out_varname})
    ds_day.attrs["history"] += f"\nresample date: {time.ctime()} AKST"
    # update local attrs
    ds_day[out_varname].attrs.update(temporal_resampling=(
        f"Daily: these data represent {aggr} of hourly data from "
        "wrf dynamical downscaling outputs."
    ))
    # remove some old local attributes
    rm_attrs = [
        "initial_time",
        "forecast_time_units",
        "forecast_time",
        "level",
        "parameter_number",
        "parameter_table_version",
        "gds_grid_type",
        "level_indicator",
        "center",
        "grid_mapping",
        "coordinates",
    ]
    for attr in rm_attrs:
        del ds_day[out_varname].attrs[attr]

    # set output compression and encoding for serialization
    encoding = ds_day[out_varname].encoding
    encoding.update(zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32")
    ds_day[out_varname].encoding = encoding
    # write
    ds_day.to_netcdf(out_fp)

    return


if __name__ == '__main__':
    # parse some args
    parser = argparse.ArgumentParser(
        description="Resample the hourly restacked WRF outputs to daily scale"
    )
    parser.add_argument(
        "-hd", dest="hourly_dir", help="Path to directory containing subfolders by variable of restacked hourly outputs"
    )
    parser.add_argument(
        "-d", dest="daily_dir", help="Path to directory to write resampled daily files"
    )
    parser.add_argument(
        "-y", dest="year_str", help="String for years to process, in '<start year>-<end year>' format"
    )
    parser.add_argument(
        "-a", dest="aggr", help="Name of aggregation method"
    )
    parser.add_argument(
        "-wv", dest="wrf_varname", help="Name of WRF variable"
    )
    parser.add_argument(
        "-ov", dest="out_varname", help="Name of output variable"
    )
    parser.add_argument(
        "-fs",
        dest="fn_str",
        help="Substring for WRF group being worked on",
    )
    parser.add_argument(
        "-n",
        dest="ncpus",
        type=int,
        help="Number of CPUs to use for multiprocessing",
    )
    
    # parse the args and unpack
    args = parser.parse_args()
    hourly_dir = Path(args.hourly_dir)
    daily_dir = Path(args.daily_dir)
    year_str = args.year_str
    aggr = args.aggr
    wrf_varname = args.wrf_varname
    out_varname = args.out_varname
    fn_str = args.fn_str
    ncpus = args.ncpus
    
    # years to work on
    years = np.arange(int(year_str.split("-")[0]), int(year_str.split("-")[1]) + 1)
    
    # output dir is derived from hourly restack dir 
    out_dir = daily_dir.joinpath(out_varname)
    out_dir.mkdir(exist_ok=True, parents=True)
    
    # generate args for pooling
    args = []
    for year in years:
        fp = hourly_dir.joinpath(wrf_varname, f"{wrf_varname}_hourly_wrf_{fn_str}_{year}.nc")
        out_fp = out_dir.joinpath(f"{out_varname}_daily_wrf_{fn_str}_{year}.nc")
        args.append((fp, aggr, wrf_varname, out_varname, out_fp))

    # run the resampling
    tic = time.perf_counter()
    with Pool(ncpus) as pool:
        pool.starmap(resample, args)

    print((
        f"Hourly files in {hourly_dir.joinpath(wrf_varname)} for {fn_str} "
        f"resampled to daily and written to {out_dir} in "
        f"{round((time.perf_counter() - tic) / 60)}m"
    ))
