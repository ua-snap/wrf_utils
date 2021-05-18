# pylint: disable=C0103, W0621

"""Run the improvement step of the pipeline to
created copies of the stacked data with user-friendly
file structure

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/wrap_run_improve_hourly_netcdf_structure.py -v <variable names> -m <model>

Returns:
    Improved stacked data written to $OUTPUT_DIR/hourly_fix
"""

# Wrapper to run the hourly WRF netcdf structure improvement
# improve the files in $OUPUT_DIR/hourly/<var>, output them
# to $OUTPUT_DIR/../hourly_fix/<var>

# designed to be run with:
# $OUTPUT_DIR=/rcs/project_data/wrf_data/wind-issue/

import argparse
import os
import subprocess
from pathlib import Path


def run(fn, command, slurm_email, ncpus=10):
    """Run the improvement script for a particular variable and model"""
    ncpus = 32  # to hold the node
    head = (
        "#!/bin/sh\n"
        + "#SBATCH --nodes=1\n"
        + f"#SBATCH --cpus-per-task={ncpus}\n"
        + "#SBATCH --account=snap\n"
        + "#SBATCH --mail-type=FAIL\n"
        + f"#SBATCH --mail-user={slurm_email}\n"
        + "#SBATCH -p main\n"
    )

    with open(fn, "w") as f:
        f.write(head + "\n" + command + "\n")

    slurm_path, basename = os.path.split(fn)
    os.chdir(slurm_path)
    subprocess.call(["sbatch", fn])
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Improve the stacked hourly WRF data")
    parser.add_argument(
        "-v",
        "--variables",
        action="store",
        dest="variables",
        type=str,
        help="String of WRF variables to work on separated by spaces",
    )
    parser.add_argument(
        "-m",
        "--model",
        action="store",
        dest="model",
        type=str,
        default="all",
        help="Model to process: either 'era', 'gfdl', or 'ccsm'. Omit to run all models",
    )
    args = parser.parse_args()
    variables = args.variables.split(" ")
    model = args.model

    out_dir = Path(os.getenv("OUTPUT_DIR"))
    slurm_email = os.getenv("SLURM_EMAIL")
    slurm_dir = out_dir.parent.joinpath("slurm")
    slurm_dir.mkdir(exist_ok=True)
    pipenv_dir = os.getcwd()

    for variable in variables:
        if variable in ["acsnow", "albedo", "pcpt", "sh2o", "smois", "swupbc"]:
            ncpus = 5
        elif variable in ["cldfra"]:
            ncpus = 3
        elif variable in ["qvapor", "t", "ght", "omega", "u", "v", "tslb"]:
            ncpus = 2
        else:
            ncpus = 10

        command = (
            f"cd {pipenv_dir}\n"
            + " ".join(
                [
                    "pipenv run python ",
                    f"{pipenv_dir}/snap_wrf_data_prep/pipeline/improve_hourly_netcdf_structure.py",
                    "-b",
                    str(out_dir.joinpath("hourly")),
                    "-v",
                    variable,
                    "-m",
                    model,
                    "-n",
                    str(ncpus),
                ]
            ),
        )
        fn = os.path.join(
            slurm_dir, f"{variable}_improve_hourlies_version_1_update.slurm"
        )
        run(fn, command, slurm_email, ncpus)
