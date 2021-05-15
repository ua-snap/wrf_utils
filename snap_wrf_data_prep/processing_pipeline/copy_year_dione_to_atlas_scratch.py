"""Copy an annual subdirectory containing raw WRF outputs
from Dione server to atlas_scratch

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/copy_year_dione_to_atlas_scratch -i $input_path -o $output_path

Notes:
    Designed to be called by the "run_*.sh" shell scripts that execute the
    stacking of the WRF into files by year and variable:
"""

import argparse
import glob
import os
import multiprocessing as mp


def copy_fn(input_fn, output_fn):
    return os.system("cp {} {}".format(input_fn, output_fn))


def run(x):
    return copy_fn(*x)


if __name__ == "__main__":
    # parse some args
    parser = argparse.ArgumentParser(
        description="stack the hourly outputs from raw WRF outputs to NetCDF files of hourlies broken up by year."
    )
    parser.add_argument(
        "-i",
        "--input_path",
        action="store",
        dest="input_path",
        type=str,
        help="input hourly directory raw directory of files to be stacked for a given year",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        action="store",
        dest="output_path",
        type=str,
        help="output directory to dump the data temporarily",
    )

    # parse the args and unpack
    args = parser.parse_args()
    input_path = args.input_path
    output_path = args.output_path

    # list the files
    files = glob.glob(os.path.join(input_path, "*.nc"))
    ncpus = 5

    # make sure the output_path is actually there...
    if not os.path.exists(output_path):
        _ = os.makedirs(output_path)

    # build args for mp
    args = [(fn, os.path.join(output_path, os.path.basename(fn))) for fn in files]

    # multiprocess
    p = mp.Pool(ncpus)
    out = p.map(run, args)
    p.close()
    p.join()
