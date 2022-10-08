# Restack 20km Alaska / surrounding region WRF runs

This is the pipeline for post-processing the 20km WRF outputs that cover Alaska and the surrounding regions (created by P Bieniek) to permit distribution on a per-variable basis instead of in the native WRF output structure of daily files containing all variables. This pipeline also includes meaningful metadata and structural improvements to make this dataset GIS-ready.

The term "restacking" applies to what is done with the hourly data slices for each variable - they are extracted from the hourly/daily files and "stacked" (concatenated / merged) into individual files, grouped by year. 

## Running the pipeline

This pipeline is designed to be run on the Chinook HPC cluster. Ease of SNAP (re-)processing here is being favored over project portability/reproducibility. E.g., many paths that would normally be configurable as environment variables are instead hard-coded. 

This project utilizes `conda` to manage dependencies. Any part of the pipeline must be run from within the environment defined by the `environment.yml` file. Create an environment from that file via:

```
conda env create -f environment.yml
```

This will create a conda environment named `wrf_utils`. Activate the environment via:

```
conda activate wrf_utils
```

**login node** - as of September 2022, this pipeline must be run from the login node on Chinook, because the compute nodes are not accessible via port-forwarding, and do not have access to the `$ARCHIVE` filesystem. Creation of the environment has only been tested on the login node as well.

### Environment variables

The following variables need to be set prior to running any part of the pipeline:

`PROJECT_DIR`

Path to the root of this repo's directory. This will be used for constructing absolute paths for work on the cluster.

`SCRATCH_DIR`

Path to the scrath directory. If on Chinook, will likely be your user directory available in `/center1/DYNDOWN` volume. Something like `/center1/DYNDOWN/kmredilla/wrf_data`.

`CONDA_INIT`

This should be a shell script for initializing conda in a blank shell that does not read the typical `.bashrc`, as is the case with new slurm jobs.

It should look like this, with the variable `CONDA_PATH` below being the path to parent folder of your conda installation, e.g. `/home/UA/kmredilla/miniconda3`:

```
__conda_setup="$('$CONDA_PATH/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "$CONDA_PATH/etc/profile.d/conda.sh" ]; then
        . "$CONDA_PATH/etc/profile.d/conda.sh"
    else
        export PATH="$CONDA_PATH/bin:$PATH"
    fi
fi
unset __conda_setup
```

`SLURM_EMAIL`

Email address to send failed slurm job notifications to. This is variable is optional - if it is not specified, the sbatch email notification option will not be used.

`WRF_GROUP`

This variable determines which of the WRF groups is being worked on for a given script/task.

### Pipeline structure

This pipeline is different from other SNAP pipelines because it applies the same logic (notebooks, scripts) to different subsets of the WRF data, controlled by the `$WRF_GROUP` variable. It consists of the following scripts and notebooks, which should be completed for each WRF group in the order listed:

1. `stage_hourly.py`: Stage the hourly WRF files for copying. Run with

```
python stage_hourly.py
```

This will determine the required directories to stage in batch from the `$WRF_GROUP` variable. 

**Note** - This process can take a very long time for a single WRF group, so it may be smart to try using a `screen` session to run it and let it churn for a day or more. 

**Also note** - Once all files are staged, they will only remain staged for a limited time! So it is best to monitor semi frequently so that you can execute the next step ASAP after they are ready. 

2. `copy_raw.ipynb`: After staging the files, run this notebook to copy these staged raw outputs to `$SCRATCH_DIR` for processing.
3. `restack_20km.ipynb`: Run this notebook only when all files have been copied to `$SCRATCH_DIR`. This notebook will orchestrate the main processing lift of restacking the hourly outputs to have the desired structure, using slurm to distirbute the work. You will need to make sure that the processing jobs have completed before proceeding to the next step. Outputs will be written to `$SCRATCH_DIR`.

**Note** - this step requires an ancillary WRF file to be present. It should  already be present at `/import/SNAP/wrf_data/project_data/wrf_data/ancillary/geo_em.d01.nc`, but this can be checked and accomplished using the [utils.ipynb](utils.ipynb) notebook if needed.

4. `resample_daily.ipynb`: When the hourly data have been restacked, run this notebook to resample the hourly data to daily. 

**Note** - there are daily WRF data outputs, but it is more straightforward to just resample the hourly outputs. 

5. `qc.ipynb`: Next, use this notebook to quality check the new data.
6. `prod_comparison.ipynb`: This notebook will compare the newly restacked data with the existing "production" data - i.e., the data that is currently saved to the base directory, `/import/SNAP/wrf_data/project_data/wrf_data`. Obviously, this should be done before replacing the exisitng production data with the new data. This dataset has been in the wild for a while now, so we want to make sure data values are the same. The following command will run that notebook but also create a static html document that can be saved in base_dir as a record of the check.

```
nbconvert --execute --to html --output $BASE_DIR/ancillary/$WRF_GROUP_production_comparison.html
```

7. Copy the files to the base directory from scratch space. 
8. Copy the files to AWS from the base directory.

Open the notebooks with either `jupyter lab` or `jupyter notebook` to start a Jupyter server, and open the notebook and follow the directions therein. 

To access this notebook via a local web browser, use the `--no-browser` flag with the `jupyter` command:

```
# Chinook login node
# This extra argument may be needed if this alone doesn't work: --ip=127.0.0.1
jupyter lab --no-browser --port=8888`
```

use port-forwarding to access it locally:

```
# local
ssh -L 8888:localhost:8888 -N username@chinook00.rcs.alaska.edu
```

Note, being on the VPN should prevent timeouts on ssh sessions with the Chinook login node!

There is one more special notebook that will be useful for generating records that the new data values themselves match the existing production data exactly before being replaced with the new data. We will use the `nbconvert` command line utility to run this notebook with each WRF group, so that we have a record of this comparison. `nbconvert` will convert this not

This notebook is used to check that newly restacked data match the current production data (data hosted on AWS) before replacing those production data with the newly restacked data. It is meant to be run once for each WRF group, because storage constraints will not allow storage of all new restacked data on scratch space for comparison at the same time. So the expected workflow is to run the following for each WRF group after a restacking run and :

```
# set an env var for the WRF group to work on
export WRF_GROUP=gfdl_hist

# run nbconvert to execute the notebook and output to html in BASE_DIR
nbconvert --execute --to html --output $BASE_DIR/ancillary/$WRF_GROUP_production_comparison.html
```


