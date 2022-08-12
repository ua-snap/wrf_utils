#!/bin/bash

# This script assumes you have miniconda3 installed in your home directory!

CONDA_DIR=$HOME/miniconda3
# this should work for 
__conda_setup="$($CONDA_DIR'/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"

if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "$CONDA_DIR/etc/profile.d/conda.sh" ]; then
        . "$CONDA_DIR/etc/profile.d/conda.sh"
    else
        export PATH="$CONDA_DIR/bin:$PATH"
    fi
fi
unset __conda_setup
