# # # PROCESS WELTY

# WELTY_VARIABLES = [ 'ACSNOW', 'HFX', 'LH', 'LWDNB', 'LWUPB', 'Q2', 'SNOWH', 'SWDNB', 'SWUPB' ] # [PCPT, SNOW, T2, CLDFRA]

# ERA-INTERIM [ONLY?]
import os, subprocess

input_path = '/storage01/pbieniek/erain/hourly'
group = 'erain'
variables = ['ACSNOW','HFX','LH','LWDNB','LWUPB','Q2','SNOWH','SWDNB','SWUPB']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_welty/hourly'

for variable in variables:
    template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( variable )
    try:
        _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
    except:
        template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( 'PCPT' )
        pass

    years = list(range(1979,2015+1))

    os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
    for year in years:
        output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
        _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])



# GFDL-HISTORICAL
import os, subprocess

input_path = '/storage01/pbieniek/gfdl/hist/hourly'
group = 'gfdl_hist'
variables = ['ACSNOW','HFX','LH','LWDNB','LWUPB','Q2','SNOWH','SWDNB','SWUPB']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_welty/hourly'

for variable in variables:
    template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable )
    try:
        _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
    except:
        template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' )
        pass

    years = list(range(1970,2006+1))

    os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
    for year in years:
        output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
        _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])


