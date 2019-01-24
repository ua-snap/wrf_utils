# # # PROCESS RELEASE OF WINDS VARIABLES...

# ERA-INTERIM
import os, subprocess

input_path = '/storage01/pbieniek/erain/hourly'
group = 'erain'
variables = ['U10','V10']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

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
        if '_erain_' in output_filename:
            output_filename = output_filename.replace( '_erain_', '_era_interim_' )
        _ = subprocess.call(['python3','stack_hourly_variable_year_winds.py','-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn, '-a', ancillary_fn ])


# GFDL-HISTORICAL
import os, subprocess

input_path = '/storage01/pbieniek/gfdl/hist/hourly'
group = 'gfdl_hist'
variables = ['U10','V10']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

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
        if '_erain_' in output_filename:
            output_filename = output_filename.replace( '_erain_', '_era_interim_' )
        _ = subprocess.call(['python3','stack_hourly_variable_year_winds.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn, '-a', ancillary_fn ])



# GFDL-RCP85
import os, subprocess

input_path = '/storage01/rtladerjr/hourly'
group = 'gfdl_rcp85'
variables = ['U10','V10']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

for variable in variables:
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
        if '_erain_' in output_filename:
            output_filename = output_filename.replace( '_erain_', '_era_interim_' )
        _ = subprocess.call(['python3','stack_hourly_variable_year_winds.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn, '-a', ancillary_fn ])


# # # # # CCSM4
# CCSM4-HISTORICAL
import os, subprocess

input_path = '/storage01/pbieniek/ccsm/hist/hourly'
group = 'ccsm_hist'
variables = ['U10','V10']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/storage01/malindgren/wrf_ccsm4/hourly_new_variables'
ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

for variable in variables:
    template_fn = '/storage01/pbieniek/ccsm/hist/monthly/monthly_{}-chist2.nc'.format( variable )
    try:
        _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
    except:
        template_fn = '/storage01/pbieniek/ccsm/hist/monthly/monthly_{}-chist2.nc'.format( 'PCPT' )
        pass

    years = list(range(1970,2006+1))

    os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
    for year in years:
        output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
        _ = subprocess.call(['python3','stack_hourly_variable_year_winds.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn, '-a', ancillary_fn ])
        

# CCSM4-RCP85
import os, subprocess

input_path = '/storage01/pbieniek/ccsm/rcp85/hourly'
group = 'ccsm_rcp85'
variables = ['U10','V10']
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/storage01/malindgren/wrf_ccsm4/hourly_new_variables'
ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

for variable in variables:
    template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-chist2.nc'.format( variable ) # USING PETERS HISTORICALS HERE...
    try:
        _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
    except:
        template_fn = '/storage01/pbieniek/ccsm/hist/monthly/monthly_{}-chist2.nc'.format( 'PCPT' ) # USING PETERS HISTORICALS HERE...
        pass

    years = list(range(2006,2100+1))

    os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
    for year in years:
        output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
        _ = subprocess.call(['python3','stack_hourly_variable_year_winds.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn, '-a', ancillary_fn ])

