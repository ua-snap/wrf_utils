"""Run a simple quality control check on some restacked (and resampled) outputs"""

import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import tqdm
# project imports
from config import *
import luts
import restack_20km as main


if __name__ == "__main__":
    
    # hourly QC
    group_fn_str = luts.groups[group]["fn_str"]
    all_wrf_fps = list(hourly_dir.glob(f"*/*{group_fn_str}*.nc"))
    args = [(fp, raw_scratch_dir) for fp in all_wrf_fps]
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