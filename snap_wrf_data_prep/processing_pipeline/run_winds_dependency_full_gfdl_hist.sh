#!/bin/sh

# this script is used for running the restacking 
# the wind variables.
# At the time of creation, it's intended use
# is for correcting the current issues with the
# mis-rotated wind variables.

# make sure the first year is already moved over before we loop
FIRSTYEAR=1970
ENDYEAR=2006
GROUPNAME=gfdl_hist
input_path=/storage01/pbieniek/gfdl/hist/hourly/${FIRSTYEAR}

output_path=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${FIRSTYEAR}
CPSCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/copy_year_dione_to_atlas_scratch.py
eval "$(conda shell.bash hook)"
conda activate
python ${CPSCRIPTNAME} -i $input_path -o $output_path;
echo "copied:${FIRSTYEAR}"
wait

for (( year=${FIRSTYEAR}; year<=${ENDYEAR}; year++ ));
    do     
        if [ ${year} -lt 2100 ]
        then
            echo ${year};
            # # copy year+1 folder to the current directory
            let CPYEAR=${year}+1;
            input_path=/storage01/pbieniek/gfdl/hist/hourly/${CPYEAR};
            output_path=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${CPYEAR};
            python ${CPSCRIPTNAME} -i ${input_path} -o ${output_path};
            echo ${input_path};
            echo ${output_path};
            echo 'copied':${CPYEAR};
        fi

        # # move to the proper pre-built .slurm files directory for the given year
        cd /atlas_scratch/kmredilla/WRF/wind-issue/restacked/slurm_scripts/${year};
        
        jid01=$(sbatch U_${year}_${GROUPNAME}.slurm);
        jid01=${jid01##* };
        jid02=$(sbatch V_${year}_${GROUPNAME}.slurm);
        jid02=${jid02##* };
        jid03=$(sbatch U10_${year}_${GROUPNAME}.slurm);
        jid03=${jid03##* };
        jid04=$(sbatch V10_${year}_${GROUPNAME}.slurm);
        jid04=${jid04##* };
        jid05=$(sbatch UBOT_${year}_${GROUPNAME}.slurm);
        jid05=${jid05##* };
        jid06=$(sbatch VBOT_${year}_${GROUPNAME}.slurm);
        jid06=${jid06##* };
        
        jobids=${jid01}:${jid02}:${jid03}:${jid04}:${jid05}:${jid06};
        depends=afterok:${jobids};
    
        # remove the year-2 folder so not until startyear ${+}2
        # this is due to accumulation vars needing to traverse adjacent years...
        let RMYEAR=${year}-2;
        let YEARTEST=${FIRSTYEAR}+2;
        if [ ${year} -gt ${YEARTEST} ]
        then
            RMSCRIPTNAME=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/remove_dir_atlas_scratch.py;
            RMDIRNAME=/atlas_scratch/kmredilla/WRF/wind-issue/${GROUPNAME}/${RMYEAR};

            srun -n 1 -p main --dependency=${depends} python ${RMSCRIPTNAME} -i ${RMDIRNAME};
            echo removed:${RMDIRNAME};
        else
            SCRIPTNAMENOTHING=/workspace/UA/kmredilla/wrf_utils/snap_wrf_data_prep/processing_pipeline/do_nothing_slurm.py;
            srun -n 1 -p main --dependency=${depends} python ${SCRIPTNAMENOTHING};
            echo "no directory removal";
        fi
    done;