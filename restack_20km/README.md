# Restack 20km Alaska / surrounding region WRF runs

This is the pipeline for post-processing the 20km WRF outputs that cover Alaska and the surrounding regions (created by P Bieniek under this [CASC-funded project](https://www.sciencebase.gov/catalog/item/5b48add3e4b060350a188aac)) to permit distribution on a per-variable basis instead of in the native WRF output structure of many-variable files containing representing a single slice in time. This pipeline also includes meaningful metadata and structural improvements to make this dataset GIS-ready.

The term "restacking" is used to describe what is done with the hourly data slices for each variable - they are extracted from the hourly files and "stacked" (concatenated / merged) into individual files, grouped by year. Thus each new file becomes a datacube for a single variable, with X, Y, and time axes. 

## Running the pipeline

This pipeline is designed to be run on the Chinook HPC cluster. Ease of SNAP (re-)processing here is being favored over project portability/reproducibility. E.g., some paths that would normally be configurable as environment variables are instead hard-coded. 

This project utilizes `conda` to manage dependencies. Any part of the pipeline must be run from within the environment defined by the `environment.yml` in the root of this repo. Create an environment from that file via:

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

This variable determines which of the WRF groups is being worked on for a given script/task. See the next section for possible values.

### WRF groups

The pipeline works on a single WRF group at one time given file access constraints. Those 5 groups are various model / scenario combinations, and are defined in the `luts.py` file. You may set the `WRF_GROUP` env var to any of the following values to process the associated dataset:

`ccsm_hist`: NCAR CCSM4 historical
`ccsm_rcp85`: NCAR CCSM4 RCP 8.5
`gfdl_hist`: GFDL CM3 historical
`gfdl_rcp85`: GFDL CM3 RCP 8.5
`erain_hist`: ERA-Interim (historical)

So to work on processing the NCAR CCSM4 historical data, run:

```
export WRF_GROUP=ccsm_hist
```

Before running any part of the pipeline.

### Pipeline structure

This pipeline is different from other SNAP pipelines because it applies the same logic (notebooks, scripts) to different subsets of the WRF data, controlled by the `$WRF_GROUP` variable; and there are multiple steps that dispatch slurm jobs! So, get ready for lots of slurming. This also contributes to the preference to break up the pipeline into multiple notebooks instead of using a single one. 

It consists of the following scripts and notebooks, which should be completed for each WRF group in the following order:

1. `stage_hourly.py`: Stage the hourly WRF files for copying. Run with

```
python stage_hourly.py
```

This will determine the required directories to stage in batch from the `$WRF_GROUP` variable. 

**Note** - This process can take a very long time for a single WRF group, so it may be smart to try using a `screen` session (login node) to run it and let it churn for a day or more. 

**Also note** - Once all files are staged, they will only remain staged for a limited time! So it is best to monitor semi-frequently so that you can execute the next step ASAP after they are ready. 

2. `copy_raw.ipynb`: After staging the files, run this notebook to copy these staged raw outputs to `$SCRATCH_DIR` for processing. You can use the [utils](utils.ipynb) notebook to monitor the progression of copying the files to scratch space.
3. `restack_20km.ipynb`: Run this notebook only when all files have been copied to `$SCRATCH_DIR`. This notebook will orchestrate the main processing lift of restacking the hourly outputs to have the desired structure, using slurm to distirbute the work. You will need to make sure that the processing jobs have completed before proceeding to the next step. Outputs will be written to `$SCRATCH_DIR`.

**Note** - this step requires an ancillary WRF file to be present. It should  already be present at `/import/SNAP/wrf_data/project_data/wrf_data/ancillary/geo_em.d01.nc`, but this file should also be available at `/import/SNAP/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc` and on other SNAP infrastructure as well.

4. `resample_daily.ipynb`: When the hourly data have been restacked, run this notebook to resample the hourly data to daily. 

**Note** - there are daily WRF data outputs existing, but it is more straightforward to just resample the hourly outputs, for purposes of preserving the new structure. 

5. `qc.ipynb`: Next, use this notebook to quality check the new data.
6. `prod_comparison.ipynb`: This notebook will compare the newly restacked data with the existing "production" data - i.e., the data that is currently saved to the base directory, `/import/SNAP/wrf_data/project_data/wrf_data/hourly` and `daily/`. Obviously, this should be done before replacing the exisitng production data with the new data. This dataset has been released for multiple years now, so we want to make sure data values are the same. This notebook will simply run a comparison which will produce results that can be viewed next. Simply run the notebook from The following command will run that notebook using the but also create a static html document that can be saved in base_dir as a record of the check.

**Note** - This can take maybe 30 minutes to and hour or more, it seems variable. Start a screen session on a compute node if you would like, and you can use the following command to run the notebook without opening it (again, after setting env vars in the new screen session):

```
jupyter nbconvert --to notebook --execute --inplace prod_comparison.ipynb
```

7. Ensure that the resulting tables created in step 6 look OK. I.e., make sure that any array or timestamp mismatches are expected. Follow the example in `ancillary/eval_prod_comparison/eval_prod_comparison_ccsm_hist.ipynb` (there may be one for each WRF group by the time you are reading this). 

8. Copy the files to the base directory from scratch space. This command should work for all WRF groups:

```
# target dir hardcoded as base_dir in config.py
# cp -r $SCRATCH_DIR/restacked /import/SNAP/wrf_data/project_data/wrf_data
# rsync option becuase it is large and rsync has some desireable properties for this:
rsync -a $SCRATCH_DIR/restacked /import/SNAP/wrf_data/project_data/wrf_data
```

**Note** - The target directory here is hardcoded in this project because it is not expected to change. I recommend starting a screen session on the login node or on a compute node for the copy, as it could take a long time. E.g.:

```
screen srun -p t1small -N 1 --pty /bin/bash
# new session on node
# set the env vars again
source restack_20km/env_vars.sh
# cp -r $SCRATCH_DIR/restacked /import/SNAP/wrf_data/project_data/wrf_data
rsync -a $SCRATCH_DIR/restacked /import/SNAP/wrf_data/project_data/wrf_data
```

8. Copy the files to AWS from the base directory.

TBD.

### Jupyter on Chinook

Open the notebooks on the Chinook login node with either `jupyter lab` or `jupyter notebook` to start a Jupyter server, and open the notebook and follow the directions therein. 

To access this notebook via a local web browser, use the `--no-browser` flag with the `jupyter` command:

```
# Chinook login node
jupyter lab --no-browser --port=8888`
```

use port-forwarding to access it locally:

```
# local
ssh -L 8888:localhost:8888 -N username@chinook00.rcs.alaska.edu
```
