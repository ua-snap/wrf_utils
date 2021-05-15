#  `wrf_utils`

This repo is used for post-processing the 20km WRF-dynamically downscaled data produced by P Bieniek et al. Colloquially referred to as "the WRF data", these data have been used in various SNAP projects and Non-snap projects.

## Contents

### `snap_wrf_data_prep/` 

This subdirectory was created to restructure the raw WRF outputs to improve useability and subsequently place them in an [AWS OpenData S3 bucket](http://wrf-ak-ar5.s3-website-us-east-1.amazonaws.com/) (metadata [here](http://ckan.snap.uaf.edu/dataset/historical-and-projected-dynamically-downscaled-climate-data-for-the-state-of-alaska-and-surrou)).

See `snap_wrf_data_prep/README.md` for info on running the WRF post-processing pipeline. 

### `wrf_smoke_processing`

Miscellaneous code for processing WRF smoke data, of unknown purpose. 

## Issues

### 1. Incorrect winds

In December 2020, developers at SNAP were informed that the ERA-Interim wind directions derived from the production dataset (the WRF data hosted on AWS in the OpenData S3 bucket linked above) were incorrect. We worked to find the issue, and were not able to identify the exact cause of the improperly rotated winds: the production code appeared to be producing the correct results, implying that the wind data hosted on AWS were not produced by the most up-to-date version of the production codebase. The `snap_wrf_data_prep/repair_winds_issue.ipynb` delves into this issue in detail, and logs the steps that were taken to repair and re-host the wind data.