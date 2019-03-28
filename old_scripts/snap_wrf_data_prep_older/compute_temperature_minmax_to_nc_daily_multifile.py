import xarray as xr
import numpy as np
import pandas as pd
import os, glob

# setup pathing, etc.
# input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'

# ERA / GFDL
variable = 't2'
# group = 'ERA-Interim'
# group = 'GFDL-CM3_historical'
# group = 'GFDL-CM3_rcp85'

# NCAR
input_path = '/storage01/malindgren/wrf_ccsm4/hourly_fix'
# group = 'NCAR-CCSM4_historical'
group = 'NCAR-CCSM4_rcp85'

# read in mfdataset
files = sorted( glob.glob( os.path.join( input_path, variable, '*'.join([variable, group,'.nc']) )) )
for fn in files:
    ds = xr.open_dataset( fn )

    # get the attrs from the input dset
    global_attrs = ds.attrs
    local_attrs = ds[ variable ].attrs
    xy_attrs = ds[ 'lon' ].attrs
    time_attrs = ds[ 'time' ].attrs

    # min metric name
    metric = 'min'

    # update the attrs for daily timestep
    local_attrs.update( temporal_resampling='Daily: these data represent {} daily outputs\
            from hourly wrf dynamical downscaling outputs.'.format( metric ),
            notes='this variable was derived from the raw hourly T2 data from wrf to generate daily {}.'.format(metric) )

    # set up some output pathing for the new aggregates
    # day
    output_path_day = os.path.join( input_path.replace( 'hourly', 'daily_TEST' ) )
    if not os.path.exists( output_path_day ):
        os.makedirs( output_path_day )

    # temp min -- daily
    ds_min = ds_max = ds.resample( time='D').min( 'time' )
    ds_min_comp = ds_min.compute()

    # setup the output dataset with proper varname and attrs
    new_varname = variable+'min'
    ds_min_comp = ds_min_comp[ variable ].to_dataset( name=new_varname )
    ds_min_comp.attrs = global_attrs
    ds_min_comp[ new_varname ].attrs = local_attrs
    ds_min_comp[ 'lon' ].attrs = xy_attrs
    ds_min_comp[ 'lat' ].attrs = xy_attrs

    encoding = ds_min_comp[ new_varname ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    ds_min_comp[ new_varname ].encoding = encoding

    # dump to disk
    dirname, basename = os.path.split( fn )
    # [ WATCH ] this is hardwired to hourly / daily...
    basename = basename.replace( 'hourly', 'daily' ).replace( '.nc', '_{}.nc'.format(metric) ).replace('t2', new_varname)
    output_filename = os.path.join( output_path_day, basename )

    ds_min_comp.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

    # cleanup...
    ds_min.close()
    ds_min = None
    ds_min_comp.close()

    # temp max -- daily
    ds_max = ds.resample( time='D').max( 'time' )
    ds_max_comp = ds_max.compute()

    # min metric name
    metric = 'max'

    # update the attrs for daily timestep
    local_attrs.update( temporal_resampling='Daily: these data represent {} daily outputs\
            from hourly wrf dynamical downscaling outputs.'.format( metric ),
            notes='this variable was derived from the raw hourly T2 data from wrf to generate daily {}.'.format(metric) )

    # setup the output dataset with proper varname and attrs
    new_varname = variable + 'max'
    ds_max_comp = ds_max_comp[ variable ].to_dataset( name=new_varname )
    ds_max_comp.attrs = global_attrs
    ds_max_comp[ new_varname ].attrs = local_attrs
    ds_max_comp[ 'lon' ].attrs = xy_attrs
    ds_max_comp[ 'lat' ].attrs = xy_attrs

    encoding = ds_max_comp[ new_varname ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    ds_max_comp[ new_varname ].encoding = encoding

    # dump to disk
    dirname, basename = os.path.split( fn )
    # [ WATCH ] this is hardwired to hourly / daily...
    basename = basename.replace( 'hourly', 'daily' ).replace( '.nc', '_{}.nc'.format(metric) ).replace('t2', new_varname)
    output_filename = os.path.join( output_path_day, basename )

    ds_max_comp.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

    # cleanup...
    ds_max.close()
    ds_max = None
    ds_max_comp.close()
    ds_max_comp = None

    # close master
    ds.close()