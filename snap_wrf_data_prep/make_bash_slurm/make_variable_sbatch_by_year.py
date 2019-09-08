def write_batch_last(fn, variable, output_filename, year):
    head = \
    "#!/bin/sh\n"+\
    "#SBATCH --wait\n"+\
    "#SBATCH --ntasks=32\n"+\
    "#SBATCH --nodes=1\n"+\
    "#SBATCH --ntasks-per-node=32\n"+\
    "#SBATCH --account=snap\n"+\
    "#SBATCH --mail-type=FAIL\n"+\
    "#SBATCH --mail-user=malindgren@alaska.edu\n"+\
    "#SBATCH -p main\n\n"

    args = "SCRIPTNAME='/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year.py'\n"+\
            "DIONEPATH='/storage01/rtladerjr/hourly'\n"+\
            "INPATH='/atlas_scratch/malindgren/WRF_DATA'\n"+\
            "FILES_DF_FN='/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv'\n" +\
            "VARIABLE={}\n".format(variable) +\
            "OUTPUT_FILENAME={}\n".format(output_filename) +\
            "TEMPLATE_FN='/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_PCPT-gfdlh.nc'\n"+\
            "YEAR={}\n\n".format(year)+\
            "ipython $SCRIPTNAME -- -i $INPATH -id $DIONEPATH -y $YEAR -f $FILES_DF_FN -v $VARIABLE -o $OUTPUT_FILENAME -t TEMPLATE_FN\n"

    with open( fn, 'w' ) as f:
        f.write( head + '\n' + args + '\n' )
    return fn


def write_batch(fn, variable, output_filename, year):
    head = \
    "#!/bin/sh\n"+\
    "#SBATCH --ntasks=32\n"+\
    "#SBATCH --nodes=1\n"+\
    "#SBATCH --ntasks-per-node=32\n"+\
    "#SBATCH --account=snap\n"+\
    "#SBATCH --mail-type=FAIL\n"+\
    "#SBATCH --mail-user=malindgren@alaska.edu\n"+\
    "#SBATCH -p main\n\n"

    args = "SCRIPTNAME='/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year.py'\n"+\
            "DIONEPATH='/storage01/rtladerjr/hourly'\n"+\
            "INPATH='/atlas_scratch/malindgren/WRF_DATA'\n"+\
            "FILES_DF_FN='/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv'\n" +\
            "VARIABLE={}\n".format(variable) +\
            "OUTPUT_FILENAME={}\n".format(output_filename) +\
            "TEMPLATE_FN='/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_PCPT-gfdlh.nc'\n"+\
            "YEAR={}\n\n".format(year)+\
            "ipython $SCRIPTNAME -- -i $INPATH -id $DIONEPATH -y $YEAR -f $FILES_DF_FN -v $VARIABLE -o $OUTPUT_FILENAME -t $TEMPLATE_FN\n"

    with open( fn, 'w' ) as f:
        f.write( head + '\n' + args + '\n' )
    return fn


if __name__ == '__main__':
    import itertools, os

    # some set up args
    group = 'gfdl_rcp85'
    variable = 'PCPT'
    output_filename= '/workspace/Shared/Tech_Projects/wrf_data/project_data/TEST_FINAL/PCPT/pcpt_test_file.nc'
    years = range(2006, 2100+1)
    variables = ['ACSNOW', 'T2', 'CANWAT', 'CLDFRA', 'HFX', 'LH', 'LWDNB', 'LWUPB', 'PCPC', 'PCPNC', 'PCPT', 'POTEVP', 
            'QBOT', 'Q2', 'SNOW', 'QVAPOR', 'SNOWC', 'SNOWH', 'SWUPB', 'SWDNB', 'TSLB', 'ALBEDO', 'VEGFRA', 
            'CLDFRA_HIGH', 'CLDFRA_LOW', 'CLDFRA_MID', 'LWUPBC', 'LWDNBC', 'GHT', 'OMEGA', 'PSFC', 'SLP', 'SH2O', 
            'SEAICE', 'SWUPBC', 'SMOIS', 'SWDNBC', 'TBOT', 'TSK', 'T']

    # write the batch file
    for year, variable in itertools.product(years, variables):
        fn = '/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/slurm_scripts/{1}/{0}_{1}_{2}.slurm'.format(variable,year,group)
        print('writing:{}'.format(os.path.basename(fn)))
        # make sure there are dirs to dump the files
        dirname = os.path.dirname(fn)
        if not os.path.exists(dirname):
            _ = os.makedirs(dirname)

        if variable != variables[-1]:
            _ = write_batch(fn, variable, output_filename, year)
        elif variable == variables[-1]:
            _ = write_batch_last(fn, variable, output_filename, year)
        else:
            BaseException('check the variables list -- something is up.')

