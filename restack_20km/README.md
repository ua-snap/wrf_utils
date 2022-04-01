# Re-stack 20km Alaska / surrounding region WRF runs

This is the pipeline for post-processing the 20km WRF outputs that cover Alaska and the surrounding regions (created by P Bieniek) to permit distribution on a per-variable basis instead of in the native WRF output structure of daily files containing all variables. This pipeline also includes meaningful metadata and structural improvements to make this dataset GIS-ready.

The term "re-stacking" applies to what is done with the hourly data slices for each variable - they are extracted from the daily files and "stacked" (concatenated / merged) into individual files, grouped by year. 

### Pipeline jupyter notebook

The `restack_20km.ipynb` notebook serves as the mechanism for executing this processing pipeline. Open it manually or via the `anaconda-project` command `anaconda-project run restack_20km` if you are doing this locally (unlikely). The markdown cells therein describe the step-by-step process for running the pipeline. 

#### Running on Chinook

This notebook was last executed on the Chinook cluster - to run and access it via a local web browser, you will need to open Jupyter `lab` or `notebook` with the `--no-browser` flag and use port-forwarding to access it locally:

```
# Chinook / remote - Note - this might only work on the login node
# depending on network configuration of compute nodes

# the --ip argument may be needed in some cases
anaconda-project run jupyter lab --no-browser --port=8888 --ip=127.0.0.1`
```
```
# local
ssh -L 8888:localhost:8888 -N kmredilla@chinook00.rcs.alaska.edu
```
... then manually open the `restack_20km.ipynb` notebook. 
