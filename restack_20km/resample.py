"""Resaple hourly file to daily. Reads a file, resamples based on provided aggregation info, writes new daily file.

Usage:
    Designed to be called via slurm scripts created by `slurm.write_sbatch_resample.py`
"""

import time
import xarray as xr
import numpy as np
import pandas as pd
import os, glob, itertools
from multiprocessing import Pool


def resample(ds, aggr, varname, out_varname, out_fp):
    """Resample a restacked hourly WRF dataset to a daily resolution using the provided aggregation function
    
    Args:
        ds (array.Dataset): hourly WRF dataset
        aggr (str): aggregation function to use
        varname (str): name of WRF variable
        out_varname (str): name of the new aggregate variable
        out_fp (pathlike): path to write resampled dataset
        
    Returns:
        day_ds (xarray.Dataset): daily WRF dataset created from hourly input
    """
    # get some attrs
    global_attrs = ds.attrs
    local_attrs = ds[varname].attrs
    time_attrs = ds.time.attrs

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

    local_attrs.update(temporal_resampling=(
        f"Daily: these data represent {aggr} of hourly data from"
        "wrf dynamical downscaling outputs."
    ))

    ds_day.attrs = global_attrs
    ds_day[varname].attrs = local_attrs
    ds_day["time"].attrs = time_attrs

    # set output compression and encoding for serialization
    encoding = ds_day[varname].encoding
    encoding.update(zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32")
    ds_day[varname].encoding = encoding
    # write
    ds_day.to_netcdf(out_fp)

    return


if __name__ == '__main__':
    # parse some args
    parser = argparse.ArgumentParser(
        description="Resample the hourly restacked WRF outputs to daily scale"
    )
    parser.add_argument(
        "-d", dest="restacked_dir", help="Path to directory containing annual restacked hourly files to resample"
    )
    parser.add_argument(
        "-g", dest="group", help="Name of WRF group to grab files for in restack_dir"
    )
    parser.add_argument(
        "-a", dest="aggr", help="Name of aggregation method"
    )
    parser.add_argument(
        "-v", dest="out_varname", help="Name of output variable"
    )
    parser.add_argument(
        "-n",
        dest="ncpus"
        type=int,
        help="Number of CPUs to use for multiprocessing",
    )
    
    # parse the args and unpack
    args = parser.parse_args()
    restacked_dir = Path(args.restacked_dir)
    group = args.group
    aggr = args.aggr
    out_varname = args.out_varname
    ncpus = args.ncpus
    
    # variable name should be name of restack_dir
    varname = restacked_dir.name
    
    # output dir is derived from hourly restack dir 
    out_dir = restacked_dir.parent.parent.joinpath("daily", out_varname)
    out_dir.mkdir(exist_ok=True, parents=True)
    
    # generate args for pooling
    args = []
    for fp in restack_dir.glob(f"*{luts.groups["fn_str"]}*.nc"):
        args.append((fp, aggr, varname, out_varname))
    
    # run the resampling
    tic = time.perf_counter()
    with Pool(24) as pool:
        pool.starmap(resample, args)

    print(f"Hourly files in {restacked_dir} for {group} resampled to daily and written and written to {out_dir} in {round((time.perf_counter() - tic) / 60)}m")
