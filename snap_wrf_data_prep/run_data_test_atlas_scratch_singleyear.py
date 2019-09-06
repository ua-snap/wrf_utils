if __name__ == '__main__':
    import os, glob
    import subprocess

    # other setup vars
    #   this is just for dealing with the dataframes for accumulation variables. 
    #   but plays the same with the other variables too.
    #   yes this is a hack but it works too.
    input_path_dione = '/storage01/rtladerjr/hourly' 
    input_path = '/atlas_scratch/malindgren/WRF_DATA'

    # group names for lookups and for output_filename(s)
    group = 'gfdl_rcp85'
    group_out_name = 'GFDL-CM3_rcp85'
    files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/TEST_FINAL'
    template_fn = '/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' )
    year = 2006
    
    # the actual wrf-vars
    variables = ['ACSNOW', 'T2', 'CANWAT', 'CLDFRA', 'HFX', 'LH', 'LWDNB', 'LWUPB', 'PCPC', 'PCPNC', 'PCPT', 'POTEVP', 
                'QBOT', 'Q2', 'SNOW', 'QVAPOR', 'SNOWC', 'SNOWH', 'SWUPB', 'SWDNB', 'TSLB', 'ALBEDO', 'VEGFRA', 
                'CLDFRA_HIGH', 'CLDFRA_LOW', 'CLDFRA_MID', 'LWUPBC', 'LWDNBC', 'GHT', 'OMEGA', 'PSFC', 'SLP', 'SH2O', 
                'SEAICE', 'SWUPBC', 'SMOIS', 'SWDNBC', 'TBOT', 'TSK', 'T']

    for variable in variables:
        print('running: {}'.format(variable))
        output_filename = os.path.join(output_path, variable, '{}_wrf_hourly_{}_{}.nc'.format(variable, group_out_name, year))
        script_name = '/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year.py'
        _ = subprocess.call(['ipython',script_name,'--','-i',input_path,'-id',input_path_dione,'-y',str(year),'-f',files_df_fn,'-v',variable,'-o',output_filename,'-t',template_fn])
