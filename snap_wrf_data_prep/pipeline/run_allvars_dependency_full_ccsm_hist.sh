#!/bin/sh

# source the processing script with the correct args
# for NCAR-CCSM4 historical
# vars are exported for use in run_allvars_dependency_full.sh
export FIRSTYEAR=1970
export ENDYEAR=2005
export GROUPNAME=ccsm_hist
export INPUT_DIR=$CCSM_HIST_DIR
export input_path=$INPUT_DIR/$FIRSTYEAR
export PIPE_DIR=$(dirname $(readlink -f $BASH_SOURCE))
RUN_SCRIPT_NAME=$PIPE_DIR/run_allvars_dependency_full.sh

source $RUN_SCRIPT_NAME
