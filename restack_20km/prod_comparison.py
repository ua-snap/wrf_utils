"""Compare new restacked data with production data

This notebook is for comparing the newest restacked WRF data, created as an upgrade replacement for the existing dataset, with the existing production dataset.
It will create a table of comparison results that can be summarized and explored to help avoid erroneously replacing production data with good data.

This notebook is NOT expected to maintain functionality, as the existing production dataset could be permanently deleted/archived after ensuring the new data match it where expected. Rather, tables generated using it for each WRF group will serve as historical records for the Great Restacking of 2022, in case they may be useful at a later date.

This notebook will check consistency by comparing a random slice in time between new outputs and existing production data for every new restacked and resampled file created for a particular WRF group.

Given space constraints, it will not be feasible to run a comparison for all WRF groups at once. So this notebook should be executed after the completion of restacking, resampling, and quality checking each of the five WRF groups. Simply run this notebook as a final step before replacing the old production data with new data.

Usage:
    python prod_comparison.py -n /import/SNAP/wrf_data/project_data/wrf_data/restacked/
"""

import argparse
from multiprocessing.pool import Pool
from pathlib import Path
import tqdm
import numpy as np
import pandas as pd
import xarray as xr
import luts
from config import *
# for a type of warning that can occur when comparing times between files
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


def compare_scratch(args):
    """Run a comparison between a scratch file and a production file. 
    Test the data values of a single time slice in each of the two restacked files for equivalence, one on scratch space and the corresponding "production" file.
    
    Args:
        new_restack_fp (path_like): path to file containing restacked data to check
        restack_prod_fp (path_like): path to production file containing restacked data to compare with
    
    Returns:
        dict with keys variable, timestamp, and result as keys
    """
    new_restack_fp, restack_prod_fp = args
    varname = new_restack_fp.parent.name
    with xr.open_dataset(new_restack_fp) as check_ds:
        idx = np.random.randint(check_ds["time"].values.shape[0])
        # save time from each because those should match
        check_time = check_ds["time"].values[idx]
        check_arr = check_ds[varname].sel(time=check_time).values
    del check_ds
    
    err = None
    try:
        with xr.open_dataset(restack_prod_fp) as prod_ds:
            # using this dataset's time values in case times don't match (has happened at least once)
            prod_time = prod_ds["time"].values[idx]
            prod_arr = prod_ds[varname].sel(time=prod_time).values

            # checks to see whether all data values in single time slice match between new file and production
            arr_result = np.all(prod_arr == check_arr)
            prod_exists = True
        del prod_ds
        
    
    except FileNotFoundError:
        # if "production" version does not exist, make a note of it
        arr_result = False
        prod_exists = False
        prod_time = None
        err = "FileNotFoundError"
        
    except RuntimeError:
        # this happens because of "RuntimeError: NetCDF: HDF error".. what?
        # just do the same as above but with a different timestamp
        # maybe hitting one of the corrupt production files?
        arr_result = False
        prod_exists = True
        prod_time = None
        err = "RuntimeError"
    
    # check to see whether time values match (they should)
    time_result = prod_time == check_time
    
    # wrf_time_str = str(check_time.astype("datetime64[h]")).replace("T", "_")
    model, scenario = new_restack_fp.name.split("_")[-3:-1]
    result = {
        "varname": varname,
        "scratch_filename": new_restack_fp,
        "prod_exists": prod_exists,
        "timestamp": check_time,
        "arr_result": arr_result,
        "time_result": time_result,
        "error": err
    }
    
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", dest="new_restack_dir", type=str, help="Parent directory of newly restacked outputs to compare with production")
    args = parser.parse_args()
    new_restack_dir = Path(args.new_restack_dir)
    
    # these paths should be constant for any SNAPer running this pipeline
    # assumes all folders are created in restack_20km.ipynb
    base_dir = Path("/import/SNAP/wrf_data/project_data/wrf_data")
    # final output directory for hourly data
    restack_prod_dir = base_dir.joinpath("hourly_fix")
    # final output directory for daily data
    resample_prod_dir = base_dir.joinpath("daily")
    
    # hourly data
    args = []
    for varname in [v.lower() for v in luts.varnames]:
        fn_str = luts.groups[group]["fn_str"]
        years = luts.groups[group]["years"]
        for year in years:
            fn = f"{varname}_hourly_wrf_{fn_str}_{year}.nc"
            new_restack_fp = new_restack_dir.joinpath("hourly", varname, fn)
            restack_prod_fp = restack_prod_dir.joinpath(varname, fn)
            args.append((new_restack_fp, restack_prod_fp))
            
    np.random.seed(99709)
    with Pool(20) as pool:
        new_rows = [
            result for result in tqdm.tqdm(
                pool.imap_unordered(compare_scratch, args), total=len(args))
        ]
        
    hourly_results_df = pd.DataFrame(new_rows)
    hourly_results_fp = anc_dir.joinpath(
        "production_data_comparisons",
        f"prod_comparison_{luts.groups[group]['fn_str']}_hourly.csv"
    )
    hourly_results_df.to_csv(hourly_results_fp, index=False)
    
    
    # Daily data
    args = []
    for varname in [v.lower() for v in luts.resample_varnames]:
        fn_str = luts.groups[group]["fn_str"]
        years = luts.groups[group]["years"]
        for year in years:
            fn = f"{varname}_daily_wrf_{fn_str}_{year}.nc"
            resample_scratch_fp = new_restack_dir.joinpath("daily", varname, fn)
            resample_prod_fp = resample_prod_dir.joinpath(varname, fn)
            args.append((resample_scratch_fp, resample_prod_fp))
             
    np.random.seed(99709)
    with Pool(20) as pool:
        new_rows = [
            result for result in tqdm.tqdm(
                pool.imap_unordered(compare_scratch, args), total=len(args))
        ]
         
    daily_results_df = pd.DataFrame(new_rows)
    daily_results_fp = anc_dir.joinpath(
        "production_data_comparisons",
        f"prod_comparison_{luts.groups[group]['fn_str']}_daily.csv"
    )
    daily_results_df.to_csv(daily_results_fp, index=False)
