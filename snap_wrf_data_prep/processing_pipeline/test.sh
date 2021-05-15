#!/bin/bash
# test script for understanding how to run pipenv and conda from shell scripts


export PIPE_DIR=$(dirname $(readlink -f $BASH_SOURCE))
echo $PIPE_DIR
# export PIPE_DIR=$(dirname $SCRIPT_PATH)
# SCRIPTNAME=$PIPE_DIR/test.py
# echo $PIPE_DIR
# echo $BASH_SOURCE
#source $PIPE_DIR/test2.sh