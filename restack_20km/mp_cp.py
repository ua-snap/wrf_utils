"""Copy files from one location to another in parallel

Intended usage is for copying WRF outputs from wrf_dir to $SCRATCH_DIR via sbatch
"""

import argparse
import os
import pickle
import time
from multiprocessing import Pool
from pathlib import Path


def sys_copy(in_fp, out_fp, clobber=False):
    if clobber:
        return os.system(f"cp {in_fp} {out_fp}")
    else:
        return os.system(f"cp -n {in_fp} {out_fp}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy files in parallel"
    )
    parser.add_argument(
        "-s",
        "--src_dir",
        action="store",
        dest="src_dir",
        type=str,
        default="",
        help="Path to the flat yearly directory containing all WRF outputs for that particular year",
    )
    parser.add_argument(
        "-d",
        "--dst_dir",
        action="store",
        dest="dst_dir",
        type=str,
        default="",
        help="Path to the yearly directory in the scratch space where all files in src_dir should be copied",
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        type=int,
        default=1,
        help="Number of CPUs to use for parallel copy",
    )
    args = parser.parse_args()
    src_dir = Path(args.src_dir)
    dst_dir = Path(args.dst_dir)
    ncpus = args.ncpus

    year = src_dir.name
    print(f"Copying files from {src_dir} to {dst_dir}", flush=True, end="...")
    tic = time.perf_counter()
    
    fp_pairs = [(fp, dst_dir.joinpath(fp.name)) for fp in src_dir.glob("*.nc")]
    
    with Pool(ncpus) as p:
        out = p.starmap(sys_copy, fp_pairs)

    print(f"done. Time elapsed: {round((time.perf_counter() - tic) / 60)}m")
    