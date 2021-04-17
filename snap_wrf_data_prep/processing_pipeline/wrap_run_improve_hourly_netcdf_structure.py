# Wrapper to run the hourly WRF netcdf structure improvement
# improve the files in $BASE_DIR/hourly/<var>, output them
# to $BASE_DIR/hourly_fix/<var>

# designed to be run with:
# $BASE_DIR=/rcs/project_data/wrf_data/wind-issue/hourly

import argparse
import os
import subprocess
from pathlib import Path


def run(fn, command, ncpus=10):
    ncpus = 32  # to hold the node
    head = (
        "#!/bin/sh\n"
        + "#SBATCH --nodes=1\n"
        + "#SBATCH --cpus-per-task={}\n".format(ncpus)
        + "#SBATCH --account=snap\n"
        + "#SBATCH --mail-type=FAIL\n"
        + "#SBATCH --mail-user=kmredilla@alaska.edu\n"
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

    base_dir = Path(os.getenv("BASE_DIR"))
    slurm_dir = base_dir.parent.joinpath("slurm")
    slurm_dir.mkdir(exist_ok=True)

    for variable in variables:
        if variable in ["acsnow", "albedo", "pcpt", "sh2o", "smois", "swupbc"]:
            ncpus = 5
        elif variable in ["cldfra"]:
            ncpus = 3
        elif variable in ["qvapor", "t", "ght", "omega", "u", "v", "tslb"]:
            ncpus = 1
        else:
            ncpus = 10

        command = " ".join(
            [
                'eval "$(conda shell.bash hook)"\nconda activate\n',
                "python",
                "/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/improve_hourly_netcdf_structure.py",
                "-b",
                str(base_dir),
                "-v",
                variable,
                "-m",
                model,
                "-n",
                str(ncpus),
            ]
        )
        fn = os.path.join(
            slurm_dir,
            "{}_improve_hourlies_{}.slurm".format(variable, "version_1_update"),
        )
        run(fn, command)
