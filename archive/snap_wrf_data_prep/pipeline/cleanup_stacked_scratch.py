# pylint: disable=C0103, W0621

"""Remove stacked files from $SCRATCH_DIR,
if present in $OUTPUT_DIR/hourly

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/cleanup_stacked_scratch.py

Notes:
    Requires the following env vars: SCRATCH_DIR, OUTPUT_DIR
    This was necessary at the time of reprocessing the wind data
    given space constraints on /atlas_scratch
"""

import os
from pathlib import Path


def get_filepaths_to_remove(scratch_dir, target_dir):
    """Get filepaths to be moved"""
    target_fns = [fp.name for fp in target_dir.glob("*/*") if fp.name[-3:] == ".nc"]

    return [fp for fp in scratch_dir.glob("*/*") if fp.name in target_fns]


def remove_file(fp):
    """Remove the file and print the filepath"""
    print(f"{fp} is duplicated, removing.")
    rm_result = fp.unlink()

    return rm_result


def run_move(scratch_fps, target_dir):
    """Remove files from scratch_dir if present in target_dir"""
    # filepaths to remove
    scratch_fps = get_filepaths_to_remove(scratch_dir, target_dir)
    # run the move
    rm_results = [remove_file(fp) for fp in scratch_fps]

    return rm_results


if __name__ == "__main__":
    scratch_dir = Path(os.getenv("SCRATCH_DIR")).joinpath("stacked")
    target_dir = Path(os.getenv("OUTPUT_DIR")).joinpath("hourly")

    move_results = run_move(scratch_dir, target_dir)
