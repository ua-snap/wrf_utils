#!/bin/sh
#SBATCH --ntasks=32
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=32
#SBATCH --account=snap
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=malindgren@alaska.edu

SCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/remove_dir_atlas_scratch.py
ipython $SCRIPTNAME -- -i 