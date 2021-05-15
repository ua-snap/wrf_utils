# pylint: disable=C0103,W0621

"""Copy the "improved" WRF NetCDF files to the AWS OpenData bucket

Usage:
    pipenv run python snap_wrf_data_prep/wrap_run_move_wrf_aws_s3.py -v "u v u10 v10 ubot vbot" -s

Notes:
    Relies on FIX_DIR env var
    For uploading repaired wind data, use:
      FIX_DIR=/rcs/project_data/wrf_data/wind-issue/hourly_fix
      SLURM_DIR=/workspace/Shared/Tech_Projects/wrf_data/project_data/aws_move_slurm
"""

import argparse
import os
import glob
import subprocess


def run(fn, command, slurm_dir, ncpus=32):
    """Write the slurm script for copying files and execute"""
    head = (
        "#!/bin/sh\n"
        + "#SBATCH --ntasks={}\n".format(ncpus)
        + "#SBATCH --nodes=1\n"
        + "#SBATCH --ntasks-per-node={}\n".format(ncpus)
        + "#SBATCH --account=snap\n"
        + "#SBATCH --mail-type=FAIL\n"
        + "#SBATCH --mail-user=kmredilla@alaska.edu\n"
        + "#SBATCH -p viz,main\n"
    )

    with open(fn, "w") as f:
        f.write(head + "\n" + command + "\n")

    os.chdir(slurm_dir)
    subprocess.call(["sbatch", fn])

    return 1


if __name__ == "__main__":
    slurm_dir = os.getenv("SLURM_DIR")
    fix_dir = os.getenv("FIX_DIR")

    parser = argparse.ArgumentParser(
        description="Move the improved NetCDF files to AWS OpenData bucket."
    )
    parser.add_argument(
        "-v",
        "--variables",
        action="store",
        dest="variables",
        type=str,
        default="",
        help="' '-separated string of names of to copy",
    )
    parser.add_argument(
        "-s",
        "--sync",
        action="store_true",
        dest="s3_sync",
        default=False,
        help="Flag to 'sync' instead of 'cp'",
    )

    # parse args and upack
    args = parser.parse_args()
    # sync instead of cp
    if args.s3_sync:
        s3_command = "sync"
    else:
        s3_command = "cp"
    print(s3_command)

    # unpack variables
    if len(args.variables) > 0:
        variables = args.variables.split(" ")
    else:
        variables = [
            "tsk",
            "t2",
            "albedo",
            "acsnow",
            "canwat",
            "cldfra",
            "cldfra_high",
            "cldfra_low",
            "cldfra_mid",
            "lwupb",
            "lwdnbc",
            "lwdnb",
            "lh",
            "hfx",
            "ght",
            "pcpc",
            "potevp",
            "psfc",
            "lwupbc",
            "t",
            "v",
            "pcpnc",
            "omega",
            "seaice",
            "slp",
            "smois",
            "sh2o",
            "snowc",
            "tbot",
            "snowh",
            "snow",
            "qbot",
            "qvapor",
            "swupbc",
            "swdnbc",
            "swdnb",
            "swupb",
            "vegfra",
            "ubot",
            "u10",
            "u",
            "tslb",
            "v10",
            "vbot",
            "q2",
        ]

    for variable in variables:
        input_dir = os.path.join(fix_dir, variable)
        models = [
            "NCAR-CCSM4_historical",
            "NCAR-CCSM4_rcp85",
            "GFDL-CM3_historical",
            "GFDL-CM3_rcp85",
            "ERA-Interim_historical",
        ]

        for model in models:
            in_fps = list(glob.glob(os.path.join(input_dir, "*")))
            modname, scenario = model.split("_")
            output_remote_dir = "s3://wrf-ak-ar5/hourly/{}/{}/{}".format(
                modname, scenario, variable
            )

            command = " ".join(
                [
                    "aws",
                    "s3",
                    "--region",
                    "us-east-1",
                    s3_command,
                    input_dir,
                    output_remote_dir,
                    "--exclude",
                    '"*"',
                    "--include",
                    '"*{}*.nc"'.format(model),
                    "--no-progress",
                ]
            )

            if s3_command == "cp":
                command += " --recursive"

            fn = os.path.join(
                slurm_dir, "{}_{}_move_s3_aws.slurm".format(variable, model),
            )
            run(fn, command, slurm_dir, ncpus=10)
