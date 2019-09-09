#!/bin/sh

# make sure the first year is already moved over before we loop
input_path=/storage01/rtladerjr/hourly/2006
output_path=/atlas_scratch/malindgren/WRF_DATA/2006
# ipython /workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/copy_year_dione_to_atlas_scratch.py -- -i $input_path -o $output_path;
echo "copied:2006"
wait

for (( year=2006; year<200; year++ ));
    do     
        if [ $year -lt 2100 ]
        then
            echo "$year";
            # # copy year+1 folder to the current directory
            let CPYEAR=$year+1;
            input_path=/storage01/rtladerjr/hourly/${CPYEAR};
            output_path=/atlas_scratch/malindgren/WRF_DATA/${CPYEAR};
            # ipython /workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/copy_year_dione_to_atlas_scratch.py -- -i $input_path -o $output_path;
            echo $input_path;
            echo $output_path;
            echo 'copied':"$CPYEAR";


        # # move to the proper pre-built .slurm files directory for the given year
        cd /workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/slurm_scripts/"$year";
        echo $(pwd);

        jid01=$(sbatch ACSNOW_"$year"_gfdl_rcp85.slurm);
        jid01=${jid01##* };
        jid02=$(sbatch ALBEDO_"$year"_gfdl_rcp85.slurm);
        jid02=${jid02##* };
        # jid03=$(sbatch CANWAT_"$year"_gfdl_rcp85.slurm);
        # jid03=${jid03##* };
        # jid04=$(sbatch CLDFRA_"$year"_gfdl_rcp85.slurm);
        # jid04=${jid04##* };
        # jid05=$(sbatch CLDFRA_HIGH_"$year"_gfdl_rcp85.slurm);
        # jid05=${jid05##* };
        # jid06=$(sbatch CLDFRA_LOW_"$year"_gfdl_rcp85.slurm);
        # jid06=${jid06##* };
        # jid07=$(sbatch CLDFRA_MID_"$year"_gfdl_rcp85.slurm);
        # jid07=${jid07##* };
        # jid08=$(sbatch GHT_"$year"_gfdl_rcp85.slurm);
        # jid08=${jid08##* };
        # jid09=$(sbatch HFX_"$year"_gfdl_rcp85.slurm);
        # jid09=${jid09##* };
        # jid10=$(sbatch LH_"$year"_gfdl_rcp85.slurm);
        # jid10=${jid10##* };
        # jid11=$(sbatch LWDNB_"$year"_gfdl_rcp85.slurm);
        # jid11=${jid11##* };
        # jid12=$(sbatch LWDNBC_"$year"_gfdl_rcp85.slurm);
        # jid12=${jid12##* };
        # jid13=$(sbatch LWUPB_"$year"_gfdl_rcp85.slurm);
        # jid13=${jid13##* };
        # jid14=$(sbatch LWUPBC_"$year"_gfdl_rcp85.slurm);
        # jid14=${jid14##* };
        # jid15=$(sbatch OMEGA_"$year"_gfdl_rcp85.slurm);
        # jid15=${jid15##* };
        # jid16=$(sbatch PCPC_"$year"_gfdl_rcp85.slurm);
        # jid16=${jid16##* };
        # jid17=$(sbatch PCPNC_"$year"_gfdl_rcp85.slurm);
        # jid17=${jid17##* };
        # jid18=$(sbatch PCPT_"$year"_gfdl_rcp85.slurm);
        # jid18=${jid18##* };
        # jid19=$(sbatch POTEVP_"$year"_gfdl_rcp85.slurm);
        # jid19=${jid19##* };
        # jid20=$(sbatch PSFC_"$year"_gfdl_rcp85.slurm);
        # jid20=${jid20##* };
        # jid21=$(sbatch Q2_"$year"_gfdl_rcp85.slurm);
        # jid21=${jid21##* };
        # jid22=$(sbatch QBOT_"$year"_gfdl_rcp85.slurm);
        # jid22=${jid22##* };
        # jid23=$(sbatch QVAPOR_"$year"_gfdl_rcp85.slurm);
        # jid23=${jid23##* };
        # jid24=$(sbatch SEAICE_"$year"_gfdl_rcp85.slurm);
        # jid24=${jid24##* };
        # jid25=$(sbatch SH2O_"$year"_gfdl_rcp85.slurm);
        # jid25=${jid25##* };
        # jid26=$(sbatch SLP_"$year"_gfdl_rcp85.slurm);
        # jid26=${jid26##* };
        # jid27=$(sbatch SMOIS_"$year"_gfdl_rcp85.slurm);
        # jid27=${jid27##* };
        # jid28=$(sbatch SNOW_"$year"_gfdl_rcp85.slurm);
        # jid28=${jid28##* };
        # jid29=$(sbatch SNOWC_"$year"_gfdl_rcp85.slurm);
        # jid29=${jid29##* };
        # jid30=$(sbatch SNOWH_"$year"_gfdl_rcp85.slurm);
        # jid30=${jid30##* };
        # jid31=$(sbatch SWDNB_"$year"_gfdl_rcp85.slurm);
        # jid31=${jid31##* };
        # jid32=$(sbatch SWDNBC_"$year"_gfdl_rcp85.slurm);
        # jid32=${jid32##* };
        # jid33=$(sbatch SWUPB_"$year"_gfdl_rcp85.slurm);
        # jid33=${jid33##* };
        # jid34=$(sbatch SWUPBC_"$year"_gfdl_rcp85.slurm);
        # jid34=${jid34##* };
        # jid35=$(sbatch T_"$year"_gfdl_rcp85.slurm);
        # jid35=${jid35##* };
        # jid36=$(sbatch T2_"$year"_gfdl_rcp85.slurm);
        # jid36=${jid36##* };
        # jid37=$(sbatch TBOT_"$year"_gfdl_rcp85.slurm);
        # jid37=${jid37##* };
        # jid38=$(sbatch TSK_"$year"_gfdl_rcp85.slurm);
        # jid38=${jid38##* };
        # jid39=$(sbatch TSLB_"$year"_gfdl_rcp85.slurm);
        # jid39=${jid39##* };
        # jid40=$(sbatch VEGFRA_"$year"_gfdl_rcp85.slurm);
        # jid40=${jid40##* };

        jobids="$jid01":"$jid02":"$jid03":"$jid04":"$jid05":"$jid06":"$jid07":"$jid08":"$jid09":"$jid10":"$jid11":"$jid12":"$jid13":"$jid14":"$jid15":"$jid16":"$jid17":"$jid18":"$jid19":"$jid20":"$jid21":"$jid22":"$jid23":"$jid24":"$jid25":"$jid26":"$jid27":"$jid28":"$jid29":"$jid30":"$jid31":"$jid32":"$jid33":"$jid34":"$jid35":"$jid36":"$jid37":"$jid38":"$jid39":"$jid40";
        # jobids="$jid01":"$jid02":"$jid03":"$jid04":"$jid05"
        # echo $jobids;

        # remove the year-1 folder
        SCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/remove_dir_atlas_scratch.py;
        RMDIRNAME=/atlas_scratch/malindgren/WRF_DATA/${year};
        depends=afterok:"$jobids"

        srun -n 1 -p main --dependency=$depends ipython $SCRIPTNAME -- -i $RMDIRNAME;
    done;

