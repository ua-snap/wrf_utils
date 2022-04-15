#!/bin/sh

# this script is used for executing the stacking 
# code on a single variable for a single year.

# At the time of creation, it's intended use
# is for correcting the current issues with the
# mis-rotated wind variables, where the batch
# script failed to complete 
# (such as following a failed copy command of an output WRF file)

# arguments should be pass in the following order
# year to process
YEAR=$1
# path to directory containing annual subdirectories of hourly WRF outputs 
INPUT_DIR=$2
# the group name corresponding to the model/scenario, 
# one of erain, ccsm_hist, ccsm_rcp85, gfdl_hist, gfdl_rcp85 
GROUPNAME=$3
# variable name in upper case, e.g. U10
VAR=$4

# example usage:
# source run_winds_single_year.sh 2010 /storage01/pbieniek/gfdl/rcp85/hourly gfdl_rcp85 U10
# source run_winds_single_year.sh 1979 /storage01/pbieniek/erain erain VBOT
input_path=$INPUT_DIR/$YEAR
output_path=$SCRATCH_DIR/$GROUPNAME/$YEAR

PIPE_DIR=$(dirname $(readlink -f $BASH_SOURCE))
CPSCRIPTNAME=$PIPE_DIR/copy_year_dione_to_atlas_scratch.py

PIPENV_DIR=$(dirname $PIPE_DIR)
cd $PIPENV_DIR;
pipenv run python $CPSCRIPTNAME -i $input_path -o $output_path;
wait
echo "copied:${YEAR}"

# # move to the proper pre-built .slurm files directory for the given year
cd $SCRATCH_DIR/slurm_scripts/${YEAR};
source ${VAR}_${YEAR}_${GROUPNAME}.slurm

cd $PIPENV_DIR;
pipenv run python ${SCRIPTNAME} -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN} -a ${ANCILLARY_FN}
wait
echo "${VAR} ${YEAR} ${GROUPNAME} stacked."

# remove the directory after completion
RMSCRIPTNAME=snap_wrf_data_prep/pipeline/remove_dir_atlas_scratch.py;
RMDIRNAME=$SCRATCH_DIR/$GROUPNAME/$YEAR;

pipenv run python $RMSCRIPTNAME -i $RMDIRNAME
echo removed:$RMDIRNAME;
