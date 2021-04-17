#!/bin/sh

# this script is used for running the stacking 
# a single variable of a single year.

# At the time of creation, it's intended use
# is for correcting the current issues with the
# mis-rotated wind variables, where the "batch"
# script failed to complete 
# (probably caused by failed copy command of an output WRF file)

# arguments should be pass in the following order
YEAR=$1
GROUPDIR=$2
GROUPNAME=$3
VAR=$4
# e.g. 2004 gfdl/hist gfdl_hist U10
# e.g. 1979 erain erain U
input_path=/storage01/pbieniek/${GROUPDIR}/hourly/${YEAR}
output_path=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${YEAR}
CPSCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/copy_year_dione_to_atlas_scratch.py
eval "$(conda shell.bash hook)"
conda activate
python ${CPSCRIPTNAME} -i $input_path -o $output_path;
echo "copied:${YEAR}"
wait

# # move to the proper pre-built .slurm files directory for the given year
cd /atlas_scratch/kmredilla/WRF/wind-issue/restacked/slurm_scripts/${YEAR};
source ${VAR}_${YEAR}_${GROUPNAME}.slurm

python ${SCRIPTNAME} -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN} -a ${ANCILLARY_FN}
wait
echo "${VAR} ${YEAR} ${GROUPNAME} stacked."

# remove the directory after completion
RMSCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/remove_dir_atlas_scratch.py;
RMDIRNAME=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${YEAR};

python ${RMSCRIPTNAME} -i ${RMDIRNAME};
wait
echo removed:${RMDIRNAME};
