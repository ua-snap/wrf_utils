"""Functions for running the restack_20km pipeline

Notes

Glossary:
- path_like: a pathlib.Path object or string that can be interpreted as one.
"""

import os
import shutil
import subprocess
import time
from multiprocessing import Pool
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import luts
from config import *


def make_variable_lookup(raw_fp):
    """make a lookup table of variables and some metadata from the RAW WRF files.
    Used to create luts.var_attrs
    
    Args:
        raw_fp (path_like): path to the raw file to use for getting variable information
    
    Returns:
        dict of attributes
    """
    # raw_fp = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    raw = xr.open_dataset(raw_fp)
    dat = {
        i: {"long_name": raw[i].long_name, "units": raw[i].units}
        for i in raw.variables.mapping.keys()
    }
    # update missing variables from the list as we have found them....
    dat.update(
        PCPT={"long_name": "Total precipitation", "units": "mm"},
        QBOT={"long_name": "Specific humidity at lowest model level", "units": "kg/kg"},
    )

    return dat


def validate_group(wrf_dir, group):
    """Check that the WRF directory matches the group
    """
    parent = wrf_dir.parent
    model, scenario = group.split("_")

    #   a bit wonky because in the current setup,
    # ERA-Interim is the only model that does not
    # have a "scenario" subfolder
    if model == "erain":
        if parent.name == model:
            return True
        else:
            return False
    elif scenario != parent.name:
        return False
    elif model != parent.parent.name:
        return False
    else:
        return True


def get_wrf_fps(wrf_dir, years):
    """Get all of the hourly WRF output filepaths for given wrf directory and years
    
    Args:
        wrf_dir (pathlib.PosixPath): path to the directory containing hourly WRF files
        years (list): list of years to get filepaths for
    
    Returns:
        wrf_fps (list): list of WRF filepaths
    """
    wrf_fps = []
    for year in years:
        wrf_fps.extend(wrf_dir.joinpath(str(year)).glob("*.nc"))

    return wrf_fps


def get_year_file_modes(year_dir):
    """Helper function to get the file modes to see which ones are staged. 
    Specifically for use with check_staged
    """
    out_str = subprocess.check_output(
        ["sls", "-D", str(year_dir)], stderr=subprocess.DEVNULL
    )
    results = str(out_str)[2:-1].split("\\n\\n")[:-1]
    
    modes = []
    for result in results:
        fp = result.split(":\\n")[0]
        mode = result.split("mode: ")[1].split("  links:")[0]
        modes.append((fp, mode))
    
    return modes


def check_year_files_offline(year_dir):
    """Helper function to see if "offline" is in the output from 'sls -D' for a year's worth of files 
    """
    out_str = subprocess.check_output(
        ["sls", "-D", str(year_dir)], stderr=subprocess.DEVNULL
    )
    results = str(out_str)[2:-1].split("\\n\\n")[:-1]

    statuses = []
    for result in results:
        fp = result.split(":\\n")[0]
        offline = "offline" in result
        statuses.append((fp, offline))

    return statuses


def check_staged(wrf_dir, years):
    """Function to confirm that files in a given year or set of years are currently 'staged' and not offline. Returns the list of file paths that are not staged.
    
    Args:
        wrf_dir (pathlib.PosixPath): path to the directory containing hourly WRF files
        years (list): list of years to get filepaths for
    
    Returns:
        wrf_fps (list): list of WRF filepaths belonging to the supplied wrf_dir and years that are not staged
    """
    print("Checking for staged files")
    year_dirs = [wrf_dir.joinpath(str(year)) for year in years]
    with Pool(20) as pool:
        out = pool.map(check_year_files_offline, year_dirs)
        
    results = [out_tpl for year_statuses in out for out_tpl in year_statuses]
    unstaged_fps = [out_tpl[0] for out_tpl in results if out_tpl[1] == True]
    unstaged_years = sorted(list(set([int(Path(fp).parent.name) for fp in unstaged_fps])))
    
    print(f"Requested years: {years}")
    if len(unstaged_years) == 0:
        print("All files are staged")
    else:
        print(f"Unstaged years: {unstaged_years}")
    
    return unstaged_years


def test_file_equivalence(fp1, fp2, heads=False, detail=False):
    """Check that fp1 and fp2 have the same size (and "header" info if heads), return bool (or dict of bools if detail)
    
    Args:
        fp1 (pathlib.PosixPath): path to first file
        fp2 (pathlib.PosixPath): path to second file to test equivalence
        head (bool): test that the "headers" of the files are the same
        detail (bool): return more info on what specfic equivalence tests failed
    
    Returns:
        bool or dict indicating equivalence of files
    """
    if fp1 == fp2:
        raise ValueError("fp1 and fp2 must be different")

    # check file sizes
    try:
        sizes_equal = fp1.stat().st_size == fp2.stat().st_size
    # except error for non-existing file error
    except FileNotFoundError:
        return False

    if heads:
        # check file heads
        with xr.open_dataset(fp1) as ds1:
            with xr.open_dataset(fp2) as ds2:
                heads_equal = str(ds1) == str(ds2)

        if not detail:
            return np.all([sizes_equal, heads_equal])
        else:
            return {"sizes_equal": sizes_equal, "heads_equal": heads_equal}

    else:
        return sizes_equal


def check_raw_scratch_file(wrf_fp, group, raw_scratch_dir):
    scratch_fp = raw_scratch_dir.joinpath(group, wrf_fp.parent.name, wrf_fp.name)
    if scratch_fp.exists():
        if test_file_equivalence(wrf_fp, scratch_fp):
            return scratch_fp
        else:
            return None


def check_raw_scratch(wrf_dir, group, years, raw_scratch_dir, ncpus=24):
    """Check to see the number of requested and missing WRF files in the raw scratch directory
    """
    # see if we can pool this?
    existing_scratch_fps = []
    all_wrf_fps = []
    for year in years:
        wrf_fps = get_wrf_fps(wrf_dir, [year])
        all_wrf_fps.extend(wrf_fps)
        args = [(fp, group, raw_scratch_dir) for fp in wrf_fps]
        with Pool(ncpus) as pool:
            existing_scratch_fps.extend(pool.starmap(check_raw_scratch_file, args))
    # discard Nones
    existing_scratch_fps = [fp for fp in existing_scratch_fps if fp is not None]

    # get list of filenames existing on scratch
    if len(existing_scratch_fps) == 0:
        print("No files from specified years found in scratch_dir")
    else:
        existing_scratch_fns = [fp.name for fp in existing_scratch_fps]
        existing_scratch_years = [int(fp.parent.name) for fp in existing_scratch_fps]
        unique_years_tpl = np.unique(
            existing_scratch_years, return_index=True, return_counts=True
        )

        # iterate over unique years indices in wrf_fps and ensure all files for that year are present
        years_on_scratch = []
        years_missing_files = []
        for year, i, count in zip(*unique_years_tpl):
            idx = np.arange(i, i + count)
            year_on_scratch = np.all(
                [all_wrf_fps[j].name in existing_scratch_fns for j in idx]
            )
            if year_on_scratch:
                years_on_scratch.append(year)
            else:
                years_missing_files.append(year)
        
        if len(years_missing_files) == 0:
            print("All years present on scratch space")
        else:
            print(f"Years with all files present on scratch space: {years_on_scratch}")
        
            # unique_years and years should be the same
            if sorted(unique_years_tpl[0]) != sorted(years):
                missing_years = set(years).difference(set(unique_years_tpl[0]))
                print(f"Years completely missing from scratch: {missing_years}")
                years_missing_files = set(years_missing_files).difference(
                    missing_years
                )

            print(f"Years with files partially missing from scratch space: {years_missing_files}")

    return years_missing_files


def make_yearly_scratch_dirs(group, years, scratch_dir):
    """Helper function to ensure all of the directories are present in
    scratch_dir for copying raw hourly WRF files
    
    Args:
        group (str): name of WRF group for use in directory paths
        years (list): list of years (ints) to create subdirs for
        scratch_dir (pathlib.PosixPath): path to scratch_dir for making directories in
            
    Returns:
        None, makes directories if they don't exist
    """
    group_dir = scratch_dir.joinpath(group)
    _ = [
        group_dir.joinpath(str(year)).mkdir(exist_ok=True, parents=True)
        for year in years
    ]

    return


def sys_copy(args):
    """Copy function for running the copy in the pipeline instead of via slurm
    
    Args:
        args (tuple): tuple of args for unpacking for use with multiprocessing.Pool:
            (src_fp, dst_fp, clobber):
                src_fp (str): path to file to be copied
                dst_fp (str): destination path to copy file to
                clobber (str): one of "filesize", "head", or False, indicating whether to clobber only after finding that file sizes are different, file headers are different, or don't clobber, respectively. 

    Returns:
        output from os.system call to cp utility
    """
    src_fp, dst_fp, clobber = args
    if clobber:
        if clobber == "all":
            return os.system(f"cp {src_fp} {dst_fp}")
        if clobber == "filesize":
            if not test_file_equivalence(src_fp, dst_fp):
                return os.system(f"cp {src_fp} {dst_fp}")
            else:
                return
        elif clobber == "head":
            if not test_file_equivalence(src_fp, dst_fp, head=True):
                return os.system(f"cp {src_fp} {dst_fp}")
            else:
                return
    else:
        return os.system(f"cp -n {src_fp} {dst_fp}")


# functions below this line were developed to assist with ensuring hourly WRF files
#   were properly copied from source directories to scratch space


def get_file_size(fp):
    """Helper function to return a file's size in bytes
    
    Args:
        fp (pathlib.PosixPath): path to file
    
    Returns:
        size of file at fp in bytes (as int)
    """
    return fp.stat().st_size


def check_scratch_file_sizes(year_scratch_dir, ncpus=8):
    """Helper function that can be used to check the sizes of hourly WRF files in scratch_dir after batch copying is done. Helpful for finding what files (if any) did not copy successfully.
    
    Args:
        year_scratch_dir (pathlib.PosixPath): path to the annual directory of hourly WRF files within scratch_dir
        ncpus (int): number of CPUs to use for Pooling the filesize checking
        
    Returns:
        flag_fps (list): list of filepaths that were flagged as not being one of the
            common sizes of these hourly WRF files.
    """
    fps = np.array(list(year_scratch_dir.glob("*.nc")))
    # unique file sizes determined from the CCSM historical data:
    valid_sizes = [35722464, 35722484, 36272728, 36272732, 36272748, 36272752]
    with Pool(ncpus) as pool:
        sizes = np.array(pool.map(get_file_size, fps))
    flag_fps = fps[[size not in valid_sizes for size in sizes]]
    return flag_fps


def recopy_raw_scratch_files(fps, wrf_dir, ncpus=8):
    """Re-copy the raw scratch files specified.
    
    Args:
        fps (list): list of paths to raw hourly WRF files in raw_scratch_dir that should be re-copied from wrf_dir
        wrf_dir (pathlib.PosixPath): path to the directory containing the annual subdirectories of hourly WRF outputs
        ncpus (int): number of CPUs to use for parallel copy
        
    Returns:
        None, re-copies the files
    """
    args = [(wrf_dir.joinpath(fp.parent.name, fp.name), fp) for fp in fps]
    print(args)
    with Pool(ncpus) as pool:
        _ = pool.starmap(shutil.copy, args)

    return


def scrape_meta(ds):
    """Scrape all metadata from a dataset that should be
    consistent across output restacked WRF files
    
    Args:
        ds (xarray.DataSet): open dataset reader for a file to pull
            metadata from that should be the same across all files

    Returns:
        dict of file metdata
    """
    shared_global_keys = [
        "proj_parameters",
        "restacked_by",
        "version",
    ]
    meta = {
        "time": ds["time"].attrs,
        "lat": ds["lat"].attrs,
        "lon": ds["lon"].attrs,
        "xc": ds["xc"].attrs,
        "xc_shape": ds["xc"].shape,
        "yc": ds["yc"].attrs,
        "yc_shape": ds["yc"].shape,
        "crs": ds["spatial_ref"].attrs,
        "global": {key: ds.attrs[key] for key in shared_global_keys},
    }

    return meta


def validate_restacked_file(args):
    """Compares the values of restacked data with those in original raw output file for a (random) time slice, and collects the file metadata for subsequent validation.
    
    Args:
        args (tuple): argument tuple consisting of the following:
            restack_fp (pathlib.PosixPath): path to file containing restacked data to check
            raw_scratch_dir (pathlib.PosixPath): path to the scratch directory containing raw output data
    
    Returns:
        dict with keys model, scenario, variable, timestamp, and match
    """
    # unpack (for pooling)
    restack_fp, raw_scratch_dir = args
    varname = restack_fp.parent.name
    # only check the actual data if the variable is not a wind or accum variable, 
    #  because we will expect those to be different
    check_data = varname.upper() not in luts.accum_varnames + luts.wind_varnames
        
    with xr.open_dataset(restack_fp) as ds:
        # collect metadata that should be consistent between files
        meta = scrape_meta(ds)
    
        if check_data:
            idx = np.random.randint(ds.time.values.shape[0])
            check_time = ds.time.values[idx]
            # check to see if this is a 3D (or more) variable.
            #  If so, randomly select and index for each extra dimension
            #  so that we are only comparing a single 2D slice
            sel_di = {"time": check_time}
            if len(ds[varname].dims) > 3:
                # first dim should be time, next will be "level" or equivalent if present
                d3_name = ds[varname].dims[1]
                d3_value = np.random.choice(ds[d3_name].values)
                sel_di.update({d3_name: d3_value})
            check_arr = ds[varname].sel(sel_di).values
    
    model, scenario = restack_fp.name.split("_")[-3:-1]
    if check_data:
        year = check_time.astype("datetime64[Y]")
        wrf_time_str = str(check_time.astype("datetime64[h]")).replace("T", "_")
        group = luts.group_fn_lu[f"{model}_{scenario}"]
        raw_fp = list(raw_scratch_dir.joinpath(f"{group}/{year}").glob(f"*{wrf_time_str}*"))[0]
        with xr.open_dataset(raw_fp) as ds:
            if len(sel_di.keys()) > 1:
                raw_d3_name = luts.rev_levelnames[d3_name]
                raw_arr = ds[varname.upper()].sel({raw_d3_name: d3_value}).values
            else:
                raw_arr = ds[varname.upper()].values

        check_result = np.all(np.flipud(raw_arr) == check_arr)
    else:
        wrf_time_str = None
        check_result = None

    result = {
        "model": model,
        "scenario": scenario,
        "variable": varname,
        "timestamp": wrf_time_str,
        "match": check_result,
        "meta": meta,
    }
    
    return result


def validate_resampled_file(args):
    """Compares the values of restacked data with those in original raw output file for a (random) time slice, and collects the file metadata for subsequent validation.
    
    Args:
        args (tuple): argument tuple consisting of the following:
            daily_fp (pathlib.PosixPath): path to file containing resampled daily data to check
            hourly_dir (pathlib.PosixPath): path to the scratch directory containing restacked hourly data
    
    Returns:
        dict with keys model, scenario, variable, timestamp, and match
    """
    # unpack (for pooling)
    daily_fp, hourly_dir = args
    resample_varname = daily_fp.parent.name
    wrf_varname = luts.resample_varnames[resample_varname.upper()]["wrf_varname"]
    aggr = luts.resample_varnames[resample_varname.upper()]["aggr"]

    with xr.open_dataset(daily_fp) as ds:
        # collect metadata that should be consistent between files
        meta = scrape_meta(ds)
        # choose random day and read data
        idx = np.random.randint(ds.time.values.shape[0])
        check_time = ds.time.values[idx]
        # Should not have to worry about another "level" like we do
        #  with the hourly validation
        check_arr = ds[resample_varname].sel(time=check_time).values

    model, scenario = daily_fp.name.split("_")[-3:-1]
    year = int(daily_fp.name.split("_")[-1].split(".")[0])
    hourly_fp = hourly_dir.joinpath(
        wrf_varname, 
        daily_fp.name.replace("daily", "hourly").replace(resample_varname, wrf_varname)
    )

    # wrf_time_str = str(check_time.astype("datetime64[h]")).replace("T", "_")
    # group = luts.group_fn_lu[f"{model}_{scenario}"]
    # raw_fp = list(raw_scratch_dir.joinpath(f"{group}/{year}").glob(f"*{wrf_time_str}*"))[0]
    
    # convert timestamp with HMS to only YMD
    check_time = pd.to_datetime(check_time).strftime('%Y-%m-%d')
    with xr.open_dataset(hourly_fp) as ds:
        # evaluate the method stored in aggr across the time dimension
        hourly_arr = getattr(ds[wrf_varname].sel(time=check_time), aggr)(dim="time").values

    check_result = np.all(hourly_arr == check_arr)

    result = {
        "model": model,
        "scenario": scenario,
        "variable": resample_varname,
        "timestamp": check_time,
        "match": check_result,
        "meta": meta,
    }

    return result


def get_year_fn_str(years):
    """Get a string to be used in filenames from a list of years
    """
    if len(years) > 1:
        if np.all(np.diff(sorted(years)) == 1):
            year_fn_str = f"{years[0]}-{years[-1]}"
        else:
            year_fn_str = "_".join([str(year) for year in years])
    elif len(years) == 1:
        year_fn_str = str(years[0])
        
    return year_fn_str


def get_mismatch_params(results_df, query_str):
    """Function for getting the parameters of files where a mismatch was found
    Should not occur, was used for troubleshooting. Supply the results dataframe from 
    combined results of validate_restacked_file, and a query string to susbet the
    dataframe.
    
    Args:
        results_df (pandas.DataFrame): resulting dataframe from combining output
            from validate_restacked_file
        query_str (str): additional query fragment for subsetting dataframe
    
    Returns
        tuple of the form (variable name, timestamp, year, date, hour)
    """
    bad_case = results_df.query(f"match == False {query_str}").iloc[0]
    bad_ts = bad_case["timestamp"]
    bad_date, bad_hr = bad_ts.split("_")
    bad_year = bad_ts.split("-")[0]
    bad_var = bad_case["variable"]
    
    return bad_var, bad_ts, bad_year, bad_date, bad_hr


def user_input_years():
    """Create an input for the years to be worked on a for a given task. Done because some WRF groups need to be processed by chunks of years because of space constraints on scratch
    
    Returns:
        years (list): list of years to work on, derived from user input srting.
    """
    years = [1738]
    valid_years = f"{luts.groups[group]['years'][0]}-{luts.groups[group]['years'][-1]}"
    while not all([int(year) in luts.groups[group]["years"] for year in years]):
        years = input(f"Years, a ' '- separated list of years, or '-'-separated start and end year (e.g. 2005-2010) for a range of years.\nYears must be in {valid_years}. Leave blank for all years:") or luts.groups[group]['years']
        if (type(years) == str) and (len(years) > 0):
            if "-" in years:
                start_year, end_year = years.split("-")
                years = list(range(int(start_year), int(end_year) + 1))
            else:
                years = [int(year) for year in years.split(" ")]
    print(f"Years selected: {years}")
    
    return years


def user_input_variables(valid_varnames):
    varnames = [""]
    while not all ([varname.upper() in valid_varnames for varname in varnames]):
        varnames = input("Enter name(s) of WRF variable(s) to restack (leave blank for all):") or valid_varnames
        if (type(varnames) == str) and (len(varnames) > 0):
            varnames = [varname.upper() for varname in varnames.split(" ")]
            
    return varnames
