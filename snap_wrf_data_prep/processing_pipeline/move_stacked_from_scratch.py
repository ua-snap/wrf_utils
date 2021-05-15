"""Move stacked files from $SCRATCH_DIR/hourly,
containing the directories named by variable,
to $OUTPUT_DIR/hourly

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/move_stacked_from_scratch.py [-p] [-d] -n <number of CPUs>

Notes:
    Requires SCRATCH_DIR, OUTPUT_DIR
    It may be possible that using too many CPUs could cause
    lockups / race conditions with writing if the -d flag is used.
"""

import argparse
import os
import shutil
from multiprocessing import Pool
from pathlib import Path
import numpy as np


def get_filepaths_to_move(scratch_dir, target_dir):
    """Get filepaths to be moved"""
    target_fns = [fp.name for fp in target_dir.glob("*/*")]

    return [fp for fp in scratch_dir.glob("*/*") if fp.name not in target_fns]


def partition_filepaths(scratch_fps):
    """Partition filepaths into groups based on parent dir (i.e., by variable)"""
    subdirs = np.unique([fp.parent.name for fp in scratch_fps])

    return [
        [fp for fp in scratch_fps if fp.parent.name == subdir] for subdir in subdirs
    ]


def move_file(in_fp, out_fp):
    """Move by copy + remove"""
    print(f"copying {in_fp}")
    cp_result = shutil.copyfile(in_fp, out_fp)
    print(f"SCRATCH_DIR/{in_fp.parent.name}/{in_fp.name} copied to {out_fp}.")
    rm_result = in_fp.unlink()
    print(f"{in_fp} removed, move complete.")

    return (cp_result, rm_result)


def wrap_move_files(scratch_fps, target_dir):
    """Wrapper for looping to run the file moving
    for a batch of files for single subdir
    """
    target_fps = [target_dir.joinpath(fp.parent.name, fp.name) for fp in scratch_fps]
    mv_results = [
        move_file(in_fp, out_fp) for in_fp, out_fp in zip(scratch_fps, target_fps)
    ]

    return mv_results


def run_move(scratch_fps, target_dir, parallel_vars, dangerous_parallel, ncpus):
    """Create and run move commands"""
    # filepaths to move
    scratch_fps = get_filepaths_to_move(scratch_dir, target_dir)
    # create directories if needed
    existing_subdirs = [subdir.name for subdir in target_dir.glob("*")]
    missing_subdirs = np.unique(
        [fp.parent.name for fp in scratch_fps if fp.parent.name not in existing_subdirs]
    )
    _ = [target_dir.joinpath(subdir).mkdir(exist_ok=True) for subdir in missing_subdirs]

    if parallel_vars:
        # partition the files to be moved into groups by parent directory
        # this will allow safer parallel file-copying for NFS
        partitioned_fps = partition_filepaths(scratch_fps)
        args = [(fp_list, target_dir) for fp_list in partitioned_fps]
        with Pool(ncpus) as pool:
            mv_results = pool.starmap(wrap_move_files, args)

    elif dangerous_parallel:
        args = [
            (fp, target_dir.joinpath(fp.parent.name, fp.name)) for fp in scratch_fps
        ]
        with Pool(ncpus) as pool:
            mv_results = pool.starmap(move_file, args)

    else:
        mv_results = wrap_move_files(scratch_fps, target_dir)

    return mv_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Move the stacked hourly outputs from a $SCRATCH_DIR to a $TARGET_DIR"
    )
    parser.add_argument(
        "-p",
        "--parallel_vars",
        action="store_true",
        dest="parallel_vars",
        default=False,
        help="Copy files in parallel by variable (prevents parallel writing to same directory)",
    )
    parser.add_argument(
        "-d",
        "--dangerous_parallel",
        action="store_true",
        dest="dangerous_parallel",
        default=False,
        help="'Dangerously' copy files in parallel (does not prevent parallel writing to same directory, which can cause lockup on certain file systems). Overridden by -p.",
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        default=6,
        type=int,
        help="Number of CPUs to use (ignored if by default)",
    )
    args = parser.parse_args()
    parallel_vars = args.parallel_vars
    dangerous_parallel = args.dangerous_parallel
    ncpus = args.ncpus

    # unset dangerous parallel if parallel_vars is set
    if parallel_vars:
        dangerous_parallel = False

    scratch_dir = Path(os.getenv("SCRATCH_DIR")).joinpath("hourly")
    target_dir = Path(os.getenv("OUTPUT_DIR")).joinpath("hourly")

    mv_results = run_move(
        scratch_dir, target_dir, parallel_vars, dangerous_parallel, ncpus
    )
