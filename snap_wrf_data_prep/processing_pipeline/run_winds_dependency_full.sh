#!/bin/sh

# Run the WRF post-processing for wind variables only
# Copy raw WRF files to SCRATCH_DIR and execute the slurm scripts

output_path=$SCRATCH_DIR/$GROUPNAME/$FIRSTYEAR
PIPENV_DIR=$(dirname $PIPE_DIR)

# make sure the first year is already moved over before we loop
cd $PIPENV_DIR
CPSCRIPTNAME=snap_wrf_data_prep/pipeline/copy_year_dione_to_atlas_scratch.py
pipenv run python ${CPSCRIPTNAME} -i $input_path -o $output_path;
wait
echo "copied:${FIRSTYEAR}"

for (( year=${FIRSTYEAR}; year<=${ENDYEAR}; year++ ));
    do     
        if [ ${year} -lt ${ENDYEAR} ]
        then
            echo ${year};
            # # copy year+1 folder to the current directory
            let CPYEAR=${year}+1;
            input_path=$INPUT_DIR/${CPYEAR};
            output_path=$SCRATCH_DIR/${GROUPNAME}/${CPYEAR};
            python ${CPSCRIPTNAME} -i ${input_path} -o ${output_path};
            echo ${input_path};
            echo ${output_path};
            echo 'copied':${CPYEAR};
        fi

        # # move to the proper pre-built .slurm files directory for the given year
        cd $SCRATCH_DIR/slurm_scripts/$year;

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
        cd $PIPENV_DIR;
        if [ ${year} -gt ${YEARTEST} ]
        then
            RMSCRIPTNAME=$snap_wrf_data_prep/pipeline/remove_dir_atlas_scratch.py;
            RMDIRNAME=$SCRATCH_DIR/${GROUPNAME}/${RMYEAR};
            srun -n 1 -p main --dependency=${depends} pipenv run python ${RMSCRIPTNAME} -i ${RMDIRNAME};
            wait
            echo removed:${RMDIRNAME};
        else
            SCRIPTNAMENOTHING=$snap_wrf_data_prep/pipeline/do_nothing_slurm.py;
            srun -n 1 -p main --dependency=${depends} pipenv run python ${SCRIPTNAMENOTHING};
            wait
            echo "no directory removal";
        fi
    done;
