"""make the sbatch slurm scripts for processing specified WRF variables

Usage:
    pipenv run python snap_wrf_data_prep/pipeline/snap_wrf_data_prep.py -v <" "-separated string of variable names>
    omit -v if running for all variables.

Returns:
    Nothing. Writes `.slurm` scripts for sbatch to annual subdirectories
    created in SCRATCH_DIR/slurm_scripts.

Notes:
    Requires SLURM_EMAIL, BASE_DIR, SCRATCH_DIR, 
    GFDL_RCP85_DIR, CCSM_RCP85_DIR, GFDL_HIST_DIR, 
    CCSM_HIST_DIR, ERA_DIR
"""

import argparse
import itertools
import os


def write_batch_winds(
    fn, variable, output_filename, year, input_path, dione_path, group, slurm_email,
):
    head = (
        "#!/bin/sh\n"
        + "#SBATCH --nodes=1\n"
        + "#SBATCH --cpus-per-task=32\n"
        + "#SBATCH --account=snap\n"
        + "#SBATCH --mail-type=FAIL\n"
        + f"#SBATCH --mail-user={slurm_email}\n"
        + "#SBATCH -p main \n\n"
    )

    args = (
        "SCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year_winds.py\n"
        + "DIONEPATH={}\n".format(dione_path)
        + "INPATH={}\n".format(input_path)
        + f"FILES_DF_FN={base_dir}/wrf/docs/WRFDS_forecast_time_attr_{group}.csv\n"
        + "VARIABLE={}\n".format(variable)
        + "OUTPUT_FILENAME={}\n".format(output_filename)
        + "TEMPLATE_FN=/atlas_scratch/kmredilla/WRF/wind-issue/monthly_PCPT-gfdlh.nc\n"
        + "YEAR={}\n".format(year)
        + "ANCILLARY_FN=/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc\n\n"
        + 'eval "$(conda shell.bash hook)"\n'
        + "conda activate\n"
        + "python ${SCRIPTNAME} -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN} -a ${ANCILLARY_FN}\n"
    )

    with open(fn, "w") as f:
        f.write(head + "\n" + args + "\n")
    return fn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create the sbatch scripts for processing via slurm."
    )
    parser.add_argument(
        "-v",
        "--variables",
        action="store",
        dest="variables",
        type=str,
        default="",
        help="' '-separated string of variable names",
    )

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
        output_path = os.path.join(scratch_dir, restacked)
        years = range(begin, end + 1)
        variables = ["U", "V", "U10", "V10", "UBOT", "VBOT"]

        # write the batch file
        for year, variable in itertools.product(years, variables):
            output_filename = os.path.join(
                output_path,
                variable.lower(),
                "{}_wrf_hourly_{}_{}.nc".format(variable, group, year),
            )
            fn = "/atlas_scratch/kmredilla/WRF/wind-issue/restacked/slurm_scripts/{1}/{0}_{1}_{2}.slurm".format(
                variable, year, group
            )
            print("writing:{}".format(os.path.basename(fn)))
            # make sure there are dirs to dump the files
            dirname = os.path.dirname(fn)
            if not os.path.exists(dirname):
                _ = os.makedirs(dirname)

            if variable in ["U", "V", "U10", "V10", "UBOT", "VBOT"]:
                _ = write_batch_winds(
                    fn, variable, output_filename, year, input_path, dione_path, group
                )
