"""Run a simple quality control check on some restacked (and resampled) outputs

Usage:
    # e.g. if WRF group is GFDL projected data
    python qc.py -n /center1/DYNDOWN/kmredilla/wrf_data/restacked/ -r /center1/DYNDOWN/kmredilla/wrf_data/raw/gfdl_rcp85/
"""

import argparse
import time
from multiprocessing import Pool
from pathlib import Path
import numpy as np
import pandas as pd
import tqdm
# project imports
from config import group, project_dir
import luts
import restack_20km as main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-n",
        dest="new_restack_dir",
        type=str,
        help="Parent directory of newly restacked outputs to compare with production"
    )
    parser.add_argument(
        "-r",
        dest="raw_dir",
        type=str,
        help="Parent directory of WRF group containing annual folders of WRF hourly outputs"
    )
    args = parser.parse_args()
    new_restack_dir = Path(args.new_restack_dir)
    raw_dir = Path(args.raw_dir)
    group_fn_str = luts.groups[group]["fn_str"]
    
    # hourly QC
    hourly_dir = new_restack_dir.joinpath("hourly")
    all_wrf_fps = list(hourly_dir.glob(f"*/*{group_fn_str}*.nc"))
    args = [(fp, raw_dir) for fp in all_wrf_fps]
    # set random seed
    np.random.seed(907)
    
    with Pool(20) as pool:
        new_rows = [
            result for result in tqdm.tqdm(
                pool.imap_unordered(main.validate_restacked_file, args), total=len(args))
        ]
    results_df = pd.DataFrame(new_rows)

    try:
        assert ~any(results_df["match"] == False)
        assert np.all([row[1]["meta"] == results_df.iloc[0]["meta"] for row in results_df.iterrows()])
    except AssertionError:
        hourly_qc_fp = project_dir.joinpath(f"{group_fn_str}_hourly_qc_results.csv")
        results_df.to_csv(hourly_qc_fp, index=False)
        print(f"Mismatch in hourly data, results written to {hourly_qc_fp}")
        
    # daily QC
    daily_dir = new_restack_dir.joinpath("daily")
    daily_wrf_fps = list(daily_dir.glob(f"*/*{group_fn_str}*.nc"))
    daily_args = [(fp, hourly_dir) for fp in daily_wrf_fps]
    # set random seed
    np.random.seed(907)
    
    with Pool(20) as pool:
        daily_rows = [
            result for result in tqdm.tqdm(
                pool.imap_unordered(main.validate_resampled_file, daily_args), total=len(daily_args))
        ]
    daily_results_df = pd.DataFrame(daily_rows)
        
    try:
        assert ~any(daily_results_df["match"] == False)
        assert np.all([row[1]["meta"] == results_df.iloc[0]["meta"] for row in daily_results_df.iterrows()])
    except AssertionError:
        daily_qc_fp = project_dir.joinpath(f"{group_fn_str}_daily_qc_results.csv")
        daily_results_df.to_csv(daily_qc_fp, index=False)
        print(f"Mismatch in daily data, results written to {daily_qc_fp}")
        
