# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
# Run the PCPT data across the 3 variable groups for all years.
# * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 

# ERA-INTERIM
import os, subprocess

input_path = '/storage01/pbieniek/erain/hourly'
group = 'erain'
variables = ['PSFC', 'Q2', 'SEAICE']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'

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
        _ = subprocess.call(['python3','stack_hourly_variable_year_with_interpfix.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])


# GFDL-HISTORICAL
import os, subprocess

input_path = '/storage01/pbieniek/gfdl/hist/hourly'
group = 'gfdl_hist'
variables = ['PSFC', 'Q2', 'SEAICE']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'

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
        _ = subprocess.call(['python3','stack_hourly_variable_year_with_interpfix.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])


# GFDL-RCP85 --> RUN: SNOW, SNOWC, CANWAT, CLDFRA, 'CLDFRA_HIGH', 'CLDFRA_LOW', 'CLDFRA_MID', 'PSFC', 'Q2', 'SEAICE'
import os, subprocess

input_path = '/storage01/rtladerjr/hourly'
group = 'gfdl_rcp85'
variable = 'VEGFRA' #'T2'
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'
template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable ) # USING PETERS HISTORICALS HERE...
try:
    _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
except:
    template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' ) # USING PETERS HISTORICALS HERE...
    pass

years = list(range(2006,2100+1))

os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
for year in years:
    output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
    _ = subprocess.call(['python3','stack_hourly_variable_year_with_interpfix.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])
