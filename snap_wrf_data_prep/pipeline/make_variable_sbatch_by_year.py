# pylint: disable=C0103, W0621

"""make the sbatch slurm scripts for processing specified WRF variables

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/make_variable_sbatch_by_year.py -v <" "-separated string of variable names>
    omit -v if running for all variables.

Returns:
    Nothing. Writes `.slurm` scripts for sbatch to annual subdirectories
    created in SCRATCH_DIR/slurm_scripts.

Notes:
    Requires the following env vars: SLURM_EMAIL, BASE_DIR, SCRATCH_DIR,
    GFDL_RCP85_DIR, CCSM_RCP85_DIR, GFDL_HIST_DIR,
    CCSM_HIST_DIR, ERA_DIR
"""

import argparse
import itertools
import os


def write_batch(
    fn,
    variable,
    output_filename,
    year,
    input_path,
    dione_path,
    base_dir,
    pipenv_dir,
    group,
    slurm_email,
    winds_true,
):
    """Write the `.slurm` scripts to be used for batch-processing
    via `sbatch`

    Args:
        fn: output filepath of the slurm script being written
        variable: upper case name of the WRF variable
        output_filename: filepath of output from stacking script
        year: year to work on
        input_path: the path to the directory containing the
            raw WRF outputs on scratch space
        dione_path: the path to the directory containing the
            raw WRF outputs that were copied to scratch space
        base_dir: path to the base directory for the processing
        pipenv_dir: the root level of the directory containing the
            processing code. Derived from where the script was called from.
        group: the model / scenario being worked on in this
            format: <model code>_<scenario code>
        slurm_email: email address used for submitting Slurm jobs
        winds_true: variable being worked on is a wind variable

    Returns:
        The `sbatch`-ready slurm script written to fn.
    """
    head = (
        "#!/bin/sh\n"
        + "#SBATCH --nodes=1\n"
        + "#SBATCH --cpus-per-task=32\n"
        + "#SBATCH --account=snap\n"
        + "#SBATCH --mail-type=FAIL\n"
        + f"#SBATCH --mail-user={slurm_email}\n"
        + "#SBATCH -p main\n\n"
    )

    if winds_true:
        script_fn = "stack_hourly_variable_year_winds.py"
        ancillary_fn = (
            f"ANCILLARY_FN={base_dir}/ancillary_wrf_constants/geo_em.d01.nc\n\n"
        )
    else:
        script_fn = "stack_hourly_variable_year.py"
        ancillary_fn = ""

    args = (
        f"SCRIPTNAME={pipenv_dir}/snap_wrf_data_prep/pipeline/{script_fn}\n"
        + f"DIONEPATH={dione_path}\n"
        + f"INPATH={input_path}\n"
        + f"FILES_DF_FN={base_dir}/wrf/docs/WRFDS_forecast_time_attr_{group}.csv\n"
        + f"VARIABLE={variable}\n"
        + f"OUTPUT_FILENAME={output_filename}\n"
        + f"TEMPLATE_FN={scratch_dir}/monthly_PCPT-gfdlh.nc\n"
        + f"YEAR={year}\n"
        + ancillary_fn
        + f"cd {pipenv_dir}\n"
        + "pipenv run python ${SCRIPTNAME} -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN} -a ${ANCILLARY_FN}\n"
    )

    with open(fn, "w") as f:
        f.write(head + "\n" + args + "\n")
    return fn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create the sbatch scripts for running the stacking step via slurm."
    )
    parser.add_argument(
        "-v",
        "--variables",
        action="store",
        dest="variables",
        type=str,
        default="",
        help="' '-separated string of variable names. Omit to initialize for all variables.",
    )
    args = parser.parse_args()

    if len(args.variables) > 0:
        variables = args.variables.split(" ")
    else:
        variables = [
            "ACSNOW",
            "T2",
            "CANWAT",
            "CLDFRA",
            "HFX",
            "LH",
            "LWDNB",
            "LWUPB",
            "PCPC",
            "PCPNC",
            "PCPT",
            "POTEVP",
            "QBOT",
            "Q2",
            "SNOW",
            "QVAPOR",
            "SNOWC",
            "SNOWH",
            "SWUPB",
            "SWDNB",
            "TSLB",
            "ALBEDO",
            "VEGFRA",
            "CLDFRA_HIGH",
            "CLDFRA_LOW",
            "CLDFRA_MID",
            "LWUPBC",
            "LWDNBC",
            "GHT",
            "OMEGA",
            "PSFC",
            "SLP",
            "SH2O",
            "SEAICE",
            "SWUPBC",
            "SMOIS",
            "SWDNBC",
            "TBOT",
            "TSK",
            "T",
            "U",
            "V",
            "U10",
            "V10",
            "UBOT",
            "VBOT",
        ]

    slurm_email = os.getenv("SLURM_EMAIL")
    base_dir = os.getenv("BASE_DIR")
    scratch_dir = os.getenv("SCRATCH_DIR")
    pipenv_dir = os.getcwd()

    # some set up args
    groups = ["gfdl_rcp85", "ccsm_hist", "erain", "ccsm_rcp85", "gfdl_hist"]
    for group in groups:
        dione_paths = {
            "gfdl_rcp85": os.getenv("GFDL_RCP85_DIR"),
            "ccsm_rcp85": os.getenv("CCSM_RCP85_DIR"),
            "gfdl_hist": os.getenv("GFDL_HIST_DIR"),
            "ccsm_hist": os.getenv("CCSM_HIST_DIR"),
            "erain": os.getenv("ERA_DIR"),
        }

        year_ranges = {
            "gfdl_rcp85": (2006, 2100),
            "ccsm_rcp85": (2005, 2100),
            "gfdl_hist": (1970, 2006),
            "ccsm_hist": (1970, 2005),
            "erain": (1979, 2018),
        }

        begin, end = year_ranges[group]
        dione_path = dione_paths[group]
        input_path = os.path.join(scratch_dir, group)
        output_dir = os.path.join(scratch_dir, "stacked")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        years = range(begin, end + 1)

        # write the batch file
        for year, variable in itertools.product(years, variables):
            output_filename = os.path.join(
                output_dir,
                variable.lower(),
                "{}_wrf_hourly_{}_{}.nc".format(variable, group, year),
            )
            fn = "{0}/stacked/slurm_scripts/{2}/{1}_{2}_{3}.slurm".format(
                scratch_dir, variable, year, group
            )
            print(f"writing:{os.path.basename(fn)}")
            # make sure there are dirs to dump the files
            dirname = os.path.dirname(fn)
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            args = (
                fn,
                variable,
                output_filename,
                year,
                input_path,
                dione_path,
                base_dir,
                pipenv_dir,
                group,
                slurm_email,
            )
            if variable in ["U", "V", "U10", "V10", "UBOT", "VBOT"]:
                _ = write_batch(*args, winds_true=True)
            else:
                _ = write_batch(*args, winds_true=False)
