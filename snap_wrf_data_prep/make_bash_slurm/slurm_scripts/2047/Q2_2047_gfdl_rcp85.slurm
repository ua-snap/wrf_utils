#!/bin/sh
#SBATCH --ntasks=32
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=32
#SBATCH --account=snap
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=malindgren@alaska.edu
#SBATCH -p main


SCRIPTNAME='/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year.py'
DIONEPATH='/storage01/rtladerjr/hourly'
INPATH='/atlas_scratch/malindgren/WRF_DATA'
FILES_DF_FN='/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv'
VARIABLE=Q2
OUTPUT_FILENAME=/workspace/Shared/Tech_Projects/wrf_data/project_data/TEST_FINAL/PCPT/pcpt_test_file.nc
TEMPLATE_FN='/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_PCPT-gfdlh.nc'
YEAR=2047

ipython $SCRIPTNAME -- -i $INPATH -id $DIONEPATH -y $YEAR -f $FILES_DF_FN -v $VARIABLE -o $OUTPUT_FILENAME -t TEMPLATE_FN

