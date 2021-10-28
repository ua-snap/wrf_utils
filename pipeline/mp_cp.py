"""Copy files from one location to another in parallel

Intended usage is for copying WRF outputs from wrf_dir to $SCRATCH_DIR via SBATCH
"""

import argparse
import os
import pickle
import time
from multiprocessing import Pool
from pathlib import Path
import tqdm

def sys_copy(in_fp, out_fp):
    return os.system(f"cp {in_fp} {out_fp}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy files in parallel"
    )
    parser.add_argument(
        "-fp",
        "--pairs_fp",
        action="store",
        dest="pairs_fp",
        type=str,
        default="",
        help="Filepath to pickled list of filepath pairs (in/out to cp command)",
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
    pairs_fp = Path(args.pairs_fp)
    ncpus = args.ncpus
    
    # unpickle the pickled list of filepath pairs
    with open(pairs_fp, "rb") as f:
        fp_pairs = pickle.load(f)
    # delete that file   
    pairs_fp.unlink()

    # get unqiue years being processed
    years = sorted(list(set([pair[0].parent.name for pair in fp_pairs])))
    print(f"Copying files for {', '.join(years)} to $SCRATCH_DIR", flush=True)
    tic = time.perf_counter()

    with Pool(ncpus) as p:
        out = p.starmap(sys_copy, fp_pairs)

    print(f"Time elapsed: {round((time.perf_counter() - tic) / 60)}m")
    