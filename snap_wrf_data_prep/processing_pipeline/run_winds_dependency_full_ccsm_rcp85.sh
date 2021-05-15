#!/bin/sh

# this script is used for initiating the (re)stacking 
# for the wind variables.
# It's intended use is for correcting the current issues with the
# mis-rotated wind data.

# source the processing script with the correct args
# for NCAR-CCSM4 RCP 8.5
# vars are exported for use in run_winds_dependency_full.sh
export FIRSTYEAR=2005
export ENDYEAR=2100
export GROUPNAME=ccsm_rcp85
export INPUT_DIR=$CCSM_RCP85_DIR
export input_path=$INPUT_DIR/$FIRSTYEAR
export PIPE_DIR=$(dirname $(readlink -f $BASH_SOURCE))
RUN_SCRIPT_NAME=$PIPE_DIR/run_winds_dependency_full.sh

source $RUN_SCRIPT_NAME
