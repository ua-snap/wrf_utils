#!/bin/sh

srun -N 1 -n 1 sbatch ACSNOW_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch ALBEDO_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch CANWAT_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch CLDFRA_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch CLDFRA_HIGH_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch CLDFRA_LOW_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch CLDFRA_MID_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch GHT_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch HFX_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch LH_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch LWDNB_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch LWDNBC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch LWUPB_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch LWUPBC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch OMEGA_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch PCPC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch PCPNC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch PCPT_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch POTEVP_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch PSFC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch Q2_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch QBOT_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch QVAPOR_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SEAICE_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SH2O_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SLP_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SMOIS_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SNOW_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SNOWC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SNOWH_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SWDNB_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SWDNBC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SWUPB_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch SWUPBC_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch T2_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch TBOT_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch TSK_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch TSLB_2006_gfdl_rcp85.slurm 
srun -N 1 -n 1 sbatch VEGFRA_2006_gfdl_rcp85.slurm 


jobs="$jid01:$jid02:$jid03:$jid04:$jid05:$jid06:$jid07:$jid08:$jid09:$jid10:$jid11:$jid12:$jid13:$jid14:$jid15:$jid16:$jid17:$jid18:$jid19:$jid20:$jid21:$jid22:$jid23:$jid24:$jid25:$jid26:$jid27:$jid28:$jid29:$jid30:$jid31:$jid32:$jid33:$jid34:$jid35:$jid36:$jid37:$jid38:$jid39:$jid40"
echo $jobs
# jid999=$(sbatch --dependency=afterok:$jid01:$jid02:$jid03:$jid04:$jid05:$jid06:$jid07:$jid08:$jid09:$jid10:$jid11:$jid12:$jid13:$jid14:$jid15:$jid16:$jid17:$jid18:$jid19:$jid20:$jid21:$jid22:$jid23:$jid24:$jid25:$jid26:$jid27:$jid28:$jid29:$jid30:$jid31:$jid32:$jid33:$jid34:$jid35:$jid36:$jid37:$jid38:$jid39:$jid40 T_2006_gfdl_rcp85.slurm --wait)
