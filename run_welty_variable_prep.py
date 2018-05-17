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
        if '_erain_' in output_filename:
            output_filename = output_filename.replace( '_erain_', '_era_interim_' )
        _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])



# GFDL-HISTORICAL
import os, subprocess

input_path = '/storage01/pbieniek/gfdl/hist/hourly'
group = 'gfdl_hist'
variables = ['CLDFRA'] #['ACSNOW','HFX','LH','LWDNB','LWUPB','Q2','SNOWH','SWDNB','SWUPB']
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
        if '_erain_' in output_filename:
            output_filename = output_filename.replace( '_erain_', '_era_interim_' )
        _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])


# GFDL-RCP85
import os, subprocess

input_path = '/storage01/rtladerjr/hourly'
group = 'gfdl_rcp85'
variables = ['Q2'] #,'SNOWH','SWDNB','SWUPB'] <<- do these next..  # DONE 'LWDNB','LWUPB',
files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_welty/hourly'
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
        _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])


# # # # # TO RE-RUN A PORTION OF THE YEARLY OUTPUTS SINCE I HAD TO MOVE NODES FOR TEM
# import os, subprocess

# input_path = '/storage01/rtladerjr/hourly'
# group = 'gfdl_rcp85'
# variables = ['LWUPB']
# files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
# output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_welty/hourly'
# for variable in variables:
#     template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable ) # USING PETERS HISTORICALS HERE...
#     try:
#         _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
#     except:
#         template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' ) # USING PETERS HISTORICALS HERE...
#         pass

#     years = list(range(2006,2053+1))

#     os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
#     for year in years:
#         output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
#         if '_erain_' in output_filename:
#             output_filename = output_filename.replace( '_erain_', '_era_interim_' )
#         _ = subprocess.call(['python3','stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])



