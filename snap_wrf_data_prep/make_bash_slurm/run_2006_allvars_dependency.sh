#!/bin/sh

jid01=$(sbatch ACSNOW_2006_gfdl_rcp85.slurm --job-name=array_job_test)
jid01=${jid01##* }
jid02=$(sbatch ALBEDO_2006_gfdl_rcp85.slurm --job-name=array_job_test)
jid02=${jid02##* }
# jid03=$(sbatch CANWAT_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid03=${jid03##* }
# jid04=$(sbatch CLDFRA_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid04=${jid04##* }
# jid05=$(sbatch CLDFRA_HIGH_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid05=${jid05##* }
# jid06=$(sbatch CLDFRA_LOW_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid06=${jid06##* }
# jid07=$(sbatch CLDFRA_MID_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid07=${jid07##* }
# jid08=$(sbatch GHT_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid08=${jid08##* }
# jid09=$(sbatch HFX_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid09=${jid09##* }
# jid10=$(sbatch LH_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid10=${jid10##* }
# jid11=$(sbatch LWDNB_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid11=${jid11##* }
# jid12=$(sbatch LWDNBC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid12=${jid12##* }
# jid13=$(sbatch LWUPB_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid13=${jid13##* }
# jid14=$(sbatch LWUPBC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid14=${jid14##* }
# jid15=$(sbatch OMEGA_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid15=${jid15##* }
# jid16=$(sbatch PCPC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid16=${jid16##* }
# jid17=$(sbatch PCPNC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid17=${jid17##* }
# jid18=$(sbatch PCPT_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid18=${jid18##* }
# jid19=$(sbatch POTEVP_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid19=${jid19##* }
# jid20=$(sbatch PSFC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid20=${jid20##* }
# jid21=$(sbatch Q2_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid21=${jid21##* }
# jid22=$(sbatch QBOT_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid22=${jid22##* }
# jid23=$(sbatch QVAPOR_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid23=${jid23##* }
# jid24=$(sbatch SEAICE_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid24=${jid24##* }
# jid25=$(sbatch SH2O_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid25=${jid25##* }
# jid26=$(sbatch SLP_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid26=${jid26##* }
# jid27=$(sbatch SMOIS_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid27=${jid27##* }
# jid28=$(sbatch SNOW_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid28=${jid28##* }
# jid29=$(sbatch SNOWC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid29=${jid29##* }
# jid30=$(sbatch SNOWH_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid30=${jid30##* }
# jid31=$(sbatch SWDNB_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid31=${jid31##* }
# jid32=$(sbatch SWDNBC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid32=${jid32##* }
# jid33=$(sbatch SWUPB_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid33=${jid33##* }
# jid34=$(sbatch SWUPBC_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid34=${jid34##* }
# jid36=$(sbatch T2_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid36=${jid36##* }
# jid37=$(sbatch TBOT_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid37=${jid37##* }
# jid38=$(sbatch TSK_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid38=${jid38##* }
# jid39=$(sbatch TSLB_2006_gfdl_rcp85.slurm --job-name=array_job_test)
# jid39=${jid39##* }
# jid40=$(sbatch VEGFRA_2006_gfdl_rcp85.slurm --job-name=array_job_test --wait)
# jid40=${jid40##* }


# # jobs="$jid01,$jid02,$jid03,$jid04,$jid05,$jid06,$jid07,$jid08,$jid09,$jid10,$jid11,$jid12,$jid13,$jid14,$jid15,$jid16,$jid17,$jid18,$jid19,$jid20,$jid21,$jid22,$jid23,$jid24,$jid25,$jid26,$jid27,$jid28,$jid29,$jid30,$jid31,$jid32,$jid33,$jid34,$jid36,$jid37,$jid38,$jid39,$jid40"
# # echo $jobs
joblist='"$jid01": "$jid02"'
echo $joblist
jid999=$(sbatch T_2006_gfdl_rcp85.slurm --d after:$joblist)
