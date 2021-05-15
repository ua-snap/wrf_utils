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

### Running the pipeline to repair the winds issue

**For SNAP purposes only**

The pipeline was modified during Q1 and Q2 of 2021 to facilitate execution of the processing code on only the variables needed - `u`, `v`, `u10`, `v10`, `ubot`, and `vbot`. 

The python used to install

It was run with the following environmental variable configuration: 

`BASE_DIR=/workspace/Shared/Tech_Projects/wrf_data`
`SCRATCH_DIR=/atlas_scratch/kmredilla/WRF/wind-issue`

