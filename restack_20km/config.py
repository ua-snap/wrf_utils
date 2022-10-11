"""Setup the environment for the restacking pipeline"""

import os
from pathlib import Path


# path-based required env vars will throw error if None
# path to root of this repo, for constructing absolute paths to scripts
project_dir = Path(os.getenv("PROJECT_DIR"))
scratch_dir = Path(os.getenv("SCRATCH_DIR"))
conda_init_script = Path(os.getenv("CONDA_INIT"))
try:
    slurm_email = Path(os.getenv("SLURM_EMAIL"))
except TypeError:
    slurm_email = None
group = os.getenv("WRF_GROUP")
# ensure WRF_GROUP is set
if group == None:
    raise ValueError("WRF_GROUP env var not defined")

# Hard-coded
base_dir = Path("/import/SNAP/wrf_data/project_data/wrf_data")

anc_dir = base_dir.joinpath("ancillary")
anc_dir.mkdir(exist_ok=True)

# monthly WRF file to serve as template
template_fp = anc_dir.joinpath("monthly_PCPT-gfdlh.nc")
# WRF geogrid file for correctly projecting data and rotating wind data
geogrid_fp = anc_dir.joinpath("geo_em.d01.nc")

# final output directory for data. Outputs should be copied here after vetting,
#  not written to directly in processing scripts.
output_dir = Path("/import/SNAP/wrf_data/project_data/wrf_data/restacked")
output_dir.mkdir(exist_ok=True)

# where raw wrf outputs will be copied on scratch
raw_scratch_dir = scratch_dir.joinpath("raw")
raw_scratch_dir.mkdir(exist_ok=True)

# where initially restacked data will be stored on scratch_space
restack_scratch_dir = scratch_dir.joinpath("restacked")
restack_scratch_dir.mkdir(exist_ok=True)
# the following two directories are for the hourly and daily restacked
#  data on scratch space
hourly_dir = restack_scratch_dir.joinpath("hourly")
hourly_dir.mkdir(exist_ok=True)
daily_dir = restack_scratch_dir.joinpath("daily")
daily_dir.mkdir(exist_ok=True)


# where slurm scripts and output logs will be written
slurm_dir = base_dir.joinpath("slurm")
slurm_dir.mkdir(exist_ok=True)

# PPROJECT_DIR paths
# cp_script = project_dir.joinpath("restack_20km/mp_cp.py") not used on Chinook, $ARCHIVE not accessible from compute nodes
restack_script = project_dir.joinpath("restack_20km/restack.py")
resample_script = project_dir.joinpath("restack_20km/resample.py")
forecast_times_script = project_dir.joinpath("restack_20km/forecast_times.py")
luts_fp = project_dir.joinpath("restack_20km/luts.py")

# partition and number of cores that should be used for slurm jobs
ncpus = 24
partition = "t1small"
