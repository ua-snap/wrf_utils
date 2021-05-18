# SNAP WRF data prep

This subdirectory was created to restructure the raw WRF outputs to improve useability and subsequently place them in an [AWS OpenData S3 bucket](http://wrf-ak-ar5.s3-website-us-east-1.amazonaws.com/) (metadata [here](http://ckan.snap.uaf.edu/dataset/historical-and-projected-dynamically-downscaled-climate-data-for-the-state-of-alaska-and-surrou)).

Use the scripts contained in this subdirectory and in `pipeline/` to execute the post-processing. 

## Running the pipeline

The pipeline is run via a combination of python scripts and shell scripts. This repository is set up to run with `pipenv`, so the python scripts should be run via `pipenv run python <scriptname>`, after the project's `venv` has been established via `pipenv install`.

The pipeline makes use of the following environmental variables:
* `SLURM_EMAIL`: the email address to use for reporting failed jobs
* `BASE_DIR`: main directory for storing project data (but not the WRF data) 
* `SCRATCH_DIR`: scratch directory for copying 
* **Raw WRF directories**. These variables contain the paths to the directories containing annual subdirectories of raw WRF outputs to be stacked. They are separated based on model/scenario because, for SNAP's purposes, at least one of these combinations (GFDL-CM3 RCP 8.5 data) is found at a different parent path. 
    - `ERA_DIR`: ERA-Interim
    - `GFDL_HIST_DIR`: Hisotrical GFDL-CM3
    - `CCSM_HIST_DIR`: Historical NCAR-CCSM4
    - `GFDL_RCP85_DIR`: RCP 8.5 GFDL-CM3
    - `NCAR_CCSM4_DIR`: RCP 8.5 NCAR-CCSM4
* `OUTPUT_DIR`: directory for writing stacked and improved data (`/hourly/` and `/hourly_fix/` are created here)

The pipeline also relies on the following files that should be copied to the proper locations:

* a template file, preferably a monthly WRF output file, in `$SCRATCH_DIR/`
* An ancillary WRF file, named `geo_em.d01.nc`, saved in `$BASE_DIR/ancillary_wrf_constants/`

The standard order for running the scripts in `pipeline` should be as follows:

1. `get_date_forecast_time_from_raw_hourly.py`
2. `make_variable_sbatch_by_year.py`
3. `run_allvars_dependency_full_<model_scenario>.sh` (order of models/scenarios should not matter)
4. `move_stacked_from_scratch`
5. `wrap_run_improve_hourly_netcdf_structure.py`

In case there are remaining files on the scratch space for logistical reasons, `cleanup_stacked_scratch` can be used to clean them up. 

The `wrap_run_move_aws_s3*.py` scripts can be used to transfer the processed data to an S3 bucket. 

**Note** - there are files here (e.g. `get_date_forecast_time_from_raw_hourly.py`) that have not been brought up to the same standard as others (e.g. `stack_hourly_variable_year.py`, and may need to be modified to function correctly.

