### `wrf_smoke_processing`

Miscellaneous code for processing WRF smoke data, of unknown origin/purpose. 

### `snap_wrf_data_prep`

The pipeline for restacking the 20 km WRF data prior to the pipeline refactor in Q4 2022. 

#### Wind data issue

**For SNAP internal use**

In December 2020, developers at SNAP were informed that the ERA-Interim wind directions derived from the production dataset (the WRF data hosted on AWS in the OpenData S3 bucket linked above) were incorrect. We worked to find the issue, and were not able to identify the exact cause of the improperly rotated winds: the production code appeared to be producing the correct results, implying that the wind data hosted on AWS were not produced by the most up-to-date version of the production codebase.

The `pipeline/` was brought to its current state during Q1 and Q2 of 2021 to facilitate execution of the processing code on only the variables needed - `u`, `v`, `u10`, `v10`, `ubot`, and `vbot`. Re-processing all variables was going to be too time-intensive. 

See `archive/` for the scripts that were created by modifying `pipeline/` code to work on only the winds. See `archive/snap_wrf_data_prep/wind-issue/repair_winds_issue.ipynb` for an exploration of the issue, repair, and QA/QC. 

