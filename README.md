#  `wrf_utils`

This repo is used for post-processing the 20km WRF-dynamically downscaled data produced by P Bieniek et al. Colloquially referred to as "the WRF data", this dataset has been and is currently being used in various SNAP and Non-SNAP projects.

## Contents

### `restack_20km/` 

This subdirectory was created to restructure the raw WRF outputs from the downscaling efforts referenced above, with the goal of improving useability and subsequently placing the data in an [AWS OpenData S3 bucket](http://wrf-ak-ar5.s3-website-us-east-1.amazonaws.com/) (metadata [here](http://ckan.snap.uaf.edu/dataset/historical-and-projected-dynamically-downscaled-climate-data-for-the-state-of-alaska-and-surrou)).

See `restack_20km/README.md` for info on running the WRF post-processing pipeline. 

### `wrf_smoke_processing`

Miscellaneous code for processing WRF smoke data, of unknown origin/purpose. 

## Issues

### 1. Incorrectly rotated winds

**For SNAP internal use**

In December 2020, developers at SNAP were informed that the ERA-Interim wind directions derived from the production dataset (the WRF data hosted on AWS in the OpenData S3 bucket linked above) were incorrect. We worked to find the issue, and were not able to identify the exact cause of the improperly rotated winds: the production code appeared to be producing the correct results, implying that the wind data hosted on AWS were not produced by the most up-to-date version of the production codebase.

The `pipeline/` in `snap_wrf_data_prep` was brought to its current state during Q1 and Q2 of 2021 to facilitate execution of the processing code on only the variables needed - `u`, `v`, `u10`, `v10`, `ubot`, and `vbot`. Re-processing all variables was going to be too time-intensive. 

See `wind-issue/` for the scripts that were created by modifying `pipeline/` code to work on only the winds. See `wind-issue/repair_winds_issue.ipynb` for an in depth exploration of the issue, repair, and QA/QC. 

