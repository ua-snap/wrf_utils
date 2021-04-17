# Remove stacked files from $SCRATCH_DIR,
# if present in TARGET_DIR

# designed to be run with:
# $SCRATCH_DIR=/atlas_scratch/kmredilla/WRF/wind-issue/restacked
# $TARGET_DIR=/rcs/project_data/wrf_data/wind-issue/hourly

import os
import shutil
import numpy as np
from pathlib import Path


def get_filepaths_to_remove(scratch_dir, target_dir):
    """Get filepaths to be moved"""
    target_fns = [fp.name for fp in target_dir.glob("*/*") if fp.name[-3:] == ".nc"]

    return [fp for fp in scratch_dir.glob("*/*") if fp.name in target_fns]


def remove_file(fp):
    print(f"{fp} is duplicate, removing.")
    rm_result = fp.unlink()

    return rm_result


def run_move(scratch_fps, target_dir):
    """Create and run move commands"""
    # filepaths to remove
    scratch_fps = get_filepaths_to_remove(scratch_dir, target_dir)
    # run the move
    rm_results = [remove_file(fp) for fp in scratch_fps]

    return rm_results


if __name__ == "__main__":
    scratch_dir = Path(os.getenv("SCRATCH_DIR"))
    target_dir = Path(os.getenv("TARGET_DIR"))

    move_results = run_move(scratch_dir, target_dir)
