#!/bin/sh

# source the processing script with the correct args
# for NCAR-CCSM4 RCP 8.5
# vars are exported for use in run_allvars_dependency_full.sh
export FIRSTYEAR=2005
export ENDYEAR=2100
export GROUPNAME=ccsm_rcp85
export INPUT_DIR=$CCSM_RCP85_DIR
export input_path=$INPUT_DIR/$FIRSTYEAR
export PIPE_DIR=$(dirname $(readlink -f $BASH_SOURCE))
RUN_SCRIPT_NAME=$PIPE_DIR/run_allvars_dependency_full.sh

source $RUN_SCRIPT_NAME
