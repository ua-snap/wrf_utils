#!/bin/sh

# this script is used for running the restacking 
# the wind variables.
# At the time of creation, it's intended use
# is for correcting the current issues with the
# mis-rotated wind variables, where the "batch"
# script failed to complete 
# (potentially caused by failed copy of output WRF file)

# arguments should be pass in the following order
# year to process
YEAR=$1
# path to directory containing annual subdirectories of hourly WRF outputs 
INPUT_DIR=$2
# the group name corresponding to the model/scenario, 
# one of erain, ccsm_hist, ccsm_rcp85, gfdl_hist, gfdl_rcp85 
GROUPNAME=$3

# example usage:
# source run_winds_single_year.sh 2010 /storage01/rtladerjr/gfdl/rcp85/hourly gfdl_rcp85
# source run_winds_single_year.sh 1979 /storage01/pbieniek/erain/hourly erain
input_path=$INPUT_DIR/$YEAR
output_path=$SCRATCH_DIR/$GROUPNAME/$YEAR

PIPE_DIR=$(dirname $(dirname $(readlink -f $BASH_SOURCE)))/pipeline
CPSCRIPTNAME=$PIPE_DIR/copy_year_dione_to_atlas_scratch.py

PIPENV_DIR=$(dirname $(dirname $PIPE_DIR))
cd $PIPENV_DIR;
pipenv run python $CPSCRIPTNAME -i $input_path -o $output_path;
wait
echo "copied:${YEAR}"

# # move to the proper pre-built .slurm files directory for the given year
cd $SCRATCH_DIR/stacked/slurm_scripts/${YEAR};

jid01=$(sbatch U_${YEAR}_${GROUPNAME}.slurm);
jid01=${jid01##* };
jid02=$(sbatch V_${YEAR}_${GROUPNAME}.slurm);
jid02=${jid02##* };
jid03=$(sbatch U10_${YEAR}_${GROUPNAME}.slurm);
jid03=${jid03##* };
jid04=$(sbatch V10_${YEAR}_${GROUPNAME}.slurm);
jid04=${jid04##* };
jid05=$(sbatch UBOT_${YEAR}_${GROUPNAME}.slurm);
jid05=${jid05##* };
jid06=$(sbatch VBOT_${YEAR}_${GROUPNAME}.slurm);
jid06=${jid06##* };

jobids=${jid01}:${jid02}:${jid03}:${jid04}:${jid05}:${jid06};
depends=afterok:${jobids};

# remove the directory after completion
RMSCRIPTNAME=snap_wrf_data_prep/pipeline/remove_dir_atlas_scratch.py;
RMDIRNAME=$SCRATCH_DIR/$GROUPNAME/$YEAR;

cd $PIPENV_DIR
srun -n 1 -p main --dependency=$depends pipenv run python $RMSCRIPTNAME -i $RMDIRNAME;
echo removed:$RMDIRNAME;
