#!/bin/sh

# make sure the first year is already moved over before we loop
# FIRSTYEAR=2005
FIRSTYEAR=2038
ENDYEAR=2100
GROUPNAME=ccsm_rcp85
input_path=/storage01/pbieniek/ccsm/rcp85/hourly/${FIRSTYEAR}

output_path=/atlas_scratch/malindgren/WRF_DATA/${GROUPNAME}/${FIRSTYEAR}
CPSCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/copy_year_dione_to_atlas_scratch.py
ipython ${CPSCRIPTNAME} -- -i $input_path -o $output_path;
echo "copied:${FIRSTYEAR}"
wait

for (( year=${FIRSTYEAR}; year<=${ENDYEAR}; year++ ));
    do     
        if [ ${year} -lt ${ENDYEAR} ]
        then
            echo ${year};
            # # copy year+1 folder to the current directory
            let CPYEAR=${year}+1;
            input_path=/storage01/pbieniek/ccsm/rcp85/hourly/${CPYEAR};
            output_path=/atlas_scratch/malindgren/WRF_DATA/${GROUPNAME}/${CPYEAR};
            ipython ${CPSCRIPTNAME} -- -i ${input_path} -o ${output_path};
            echo ${input_path};
            echo ${output_path};
            echo 'copied':${CPYEAR};
        fi

        # # move to the proper pre-built .slurm files directory for the given year
        cd /workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/slurm_scripts/${year};

        jid01=$(sbatch ACSNOW_${year}_${GROUPNAME}.slurm);
        jid01=${jid01##* };
        jid02=$(sbatch ALBEDO_${year}_${GROUPNAME}.slurm);
        jid02=${jid02##* };
        jid03=$(sbatch CANWAT_${year}_${GROUPNAME}.slurm);
        jid03=${jid03##* };
        jid04=$(sbatch CLDFRA_${year}_${GROUPNAME}.slurm);
        jid04=${jid04##* };
        jid05=$(sbatch CLDFRA_HIGH_${year}_${GROUPNAME}.slurm);
        jid05=${jid05##* };
        jid06=$(sbatch CLDFRA_LOW_${year}_${GROUPNAME}.slurm);
        jid06=${jid06##* };
        jid07=$(sbatch CLDFRA_MID_${year}_${GROUPNAME}.slurm);
        jid07=${jid07##* };
        jid08=$(sbatch GHT_${year}_${GROUPNAME}.slurm);
        jid08=${jid08##* };
        jid09=$(sbatch HFX_${year}_${GROUPNAME}.slurm);
        jid09=${jid09##* };
        jid10=$(sbatch LH_${year}_${GROUPNAME}.slurm);
        jid10=${jid10##* };
        jid11=$(sbatch LWDNB_${year}_${GROUPNAME}.slurm);
        jid11=${jid11##* };
        jid12=$(sbatch LWDNBC_${year}_${GROUPNAME}.slurm);
        jid12=${jid12##* };
        jid13=$(sbatch LWUPB_${year}_${GROUPNAME}.slurm);
        jid13=${jid13##* };
        jid14=$(sbatch LWUPBC_${year}_${GROUPNAME}.slurm);
        jid14=${jid14##* };
        jid15=$(sbatch OMEGA_${year}_${GROUPNAME}.slurm);
        jid15=${jid15##* };
        jid16=$(sbatch PCPC_${year}_${GROUPNAME}.slurm);
        jid16=${jid16##* };
        jid17=$(sbatch PCPNC_${year}_${GROUPNAME}.slurm);
        jid17=${jid17##* };
        jid18=$(sbatch PCPT_${year}_${GROUPNAME}.slurm);
        jid18=${jid18##* };
        jid19=$(sbatch POTEVP_${year}_${GROUPNAME}.slurm);
        jid19=${jid19##* };
        jid35=$(sbatch T_${year}_${GROUPNAME}.slurm);
        jid35=${jid35##* };
        jid42=$(sbatch V_${year}_${GROUPNAME}.slurm);
        jid42=${jid42##* };
        jid41=$(sbatch U_${year}_${GROUPNAME}.slurm);
        jid41=${jid41##* };
        jid20=$(sbatch PSFC_${year}_${GROUPNAME}.slurm);
        jid20=${jid20##* };
        jid21=$(sbatch Q2_${year}_${GROUPNAME}.slurm);
        jid21=${jid21##* };
        jid22=$(sbatch QBOT_${year}_${GROUPNAME}.slurm);
        jid22=${jid22##* };
        jid23=$(sbatch QVAPOR_${year}_${GROUPNAME}.slurm);
        jid23=${jid23##* };
        jid24=$(sbatch SEAICE_${year}_${GROUPNAME}.slurm);
        jid24=${jid24##* };
        jid25=$(sbatch SH2O_${year}_${GROUPNAME}.slurm);
        jid25=${jid25##* };
        jid26=$(sbatch SLP_${year}_${GROUPNAME}.slurm);
        jid26=${jid26##* };
        jid27=$(sbatch SMOIS_${year}_${GROUPNAME}.slurm);
        jid27=${jid27##* };
        jid28=$(sbatch SNOW_${year}_${GROUPNAME}.slurm);
        jid28=${jid28##* };
        jid29=$(sbatch SNOWC_${year}_${GROUPNAME}.slurm);
        jid29=${jid29##* };
        jid30=$(sbatch SNOWH_${year}_${GROUPNAME}.slurm);
        jid30=${jid30##* };
        jid31=$(sbatch SWDNB_${year}_${GROUPNAME}.slurm);
        jid31=${jid31##* };
        jid32=$(sbatch SWDNBC_${year}_${GROUPNAME}.slurm);
        jid32=${jid32##* };
        jid33=$(sbatch SWUPB_${year}_${GROUPNAME}.slurm);
        jid33=${jid33##* };
        jid34=$(sbatch SWUPBC_${year}_${GROUPNAME}.slurm);
        jid34=${jid34##* };
        jid36=$(sbatch T2_${year}_${GROUPNAME}.slurm);
        jid36=${jid36##* };
        jid37=$(sbatch TBOT_${year}_${GROUPNAME}.slurm);
        jid37=${jid37##* };
        jid38=$(sbatch TSK_${year}_${GROUPNAME}.slurm);
        jid38=${jid38##* };
        jid39=$(sbatch TSLB_${year}_${GROUPNAME}.slurm);
        jid39=${jid39##* };
        jid40=$(sbatch VEGFRA_${year}_${GROUPNAME}.slurm);
        jid40=${jid40##* };
        jid43=$(sbatch U10_${year}_${GROUPNAME}.slurm);
        jid43=${jid43##* };
        jid44=$(sbatch V10_${year}_${GROUPNAME}.slurm);
        jid44=${jid44##* };
        jid45=$(sbatch UBOT_${year}_${GROUPNAME}.slurm);
        jid45=${jid45##* };
        jid46=$(sbatch VBOT_${year}_${GROUPNAME}.slurm);
        jid46=${jid46##* };            

        jobids=${jid01}:${jid02}:${jid03}:${jid04}:${jid05}:${jid06}:${jid07}:${jid08}:${jid09}:${jid10}:${jid11}:${jid12}:${jid13}:${jid14}:${jid15}:${jid16}:${jid17}:${jid18}:${jid19}:${jid20}:${jid21}:${jid22}:${jid23}:${jid24}:${jid25}:${jid26}:${jid27}:${jid28}:${jid29}:${jid30}:${jid31}:${jid32}:${jid33}:${jid34}:${jid35}:${jid36}:${jid37}:${jid38}:${jid39}:${jid40}:${jid41}:${jid42}:${jid43}:${jid44}:${jid45}:${jid46};
        depends=afterok:${jobids};
    
        # remove the year-2 folder so not until startyear ${+}2
        # this is due to accumulation vars needing to traverse adjacent years...
        let RMYEAR=${year}-2;
        let YEARTEST=${FIRSTYEAR}+2;
        if [ ${year} -gt ${YEARTEST} ]
        then
            RMSCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/remove_dir_atlas_scratch.py;
            RMDIRNAME=/atlas_scratch/malindgren/WRF_DATA/${GROUPNAME}/${RMYEAR};

            srun -n 1 -p main --dependency=${depends} ipython ${RMSCRIPTNAME} -- -i ${RMDIRNAME};
            echo removed:${RMDIRNAME};
        else
            SCRIPTNAMENOTHING=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/do_nothing_slurm.py;
            srun -n 1 -p main --dependency=${depends} ipython ${SCRIPTNAMENOTHING};
            echo "no directory removal";
        fi
    done;
