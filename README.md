#  `wrf_utils`

This repo is used for post-processing the 20km WRF-dynamically downscaled data produced by P Bieniek et al. Colloquially referred to as "the WRF data", this dataset has been and is currently being used in various SNAP and Non-SNAP projects.

## Contents

### `restack_20km/` 

This subdirectory was created to restructure the raw WRF outputs from the downscaling efforts referenced above, with the goal of improving useability and subsequently placing the data in an [AWS OpenData S3 bucket](http://wrf-ak-ar5.s3-website-us-east-1.amazonaws.com/) (metadata [here](http://ckan.snap.uaf.edu/dataset/historical-and-projected-dynamically-downscaled-climate-data-for-the-state-of-alaska-and-surrou)).

See `restack_20km/README.md` for info on running the WRF post-processing pipeline. 

## `archive`

This folder contains some old code for historical visibility purposes. Information on past issues, such as incorrectly rotated winds, can be found there. 
