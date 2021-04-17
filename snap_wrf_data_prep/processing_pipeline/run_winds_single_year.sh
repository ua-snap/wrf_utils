#!/bin/sh

# this script is used for running the restacking 
# the wind variables.
# At the time of creation, it's intended use
# is for correcting the current issues with the
# mis-rotated wind variables, where the "batch"
# script failed to complete 
# (probably caused by failed copy of output WRF file)

# arguments should be pass in the following order
YEAR=$1
GROUPDIR=$2
GROUPNAME=$3
# e.g. 2004 gfdl/hist gfdl_hist
# e.g. 1979 erain erain
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
# RMSCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/remove_dir_atlas_scratch.py;
# RMDIRNAME=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${YEAR};

# srun -n 1 -p main --dependency=${depends} python ${RMSCRIPTNAME} -i ${RMDIRNAME};
# echo removed:${RMDIRNAME};
