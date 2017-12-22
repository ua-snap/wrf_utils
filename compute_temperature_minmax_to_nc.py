import xarray as xr
import numpy as np
import pandas as pd
import os, glob

# setup pathing, etc.
input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

variable = 't2'
group = 'erain'

# read in mfdataset
ds = xr.open_mfdataset( glob.glob( os.path.join( input_path, variable, '*'.join(['', variable.upper(), group,'.nc']) )) )

# get the attrs from the input dset
global_attrs = ds.attrs
local_attrs = ds[ variable.upper() ].attrs
xy_attrs = ds[ 'lon' ].attrs
# time_attrs = ds[ 'time' ].attrs

# set up some output pathing for the new aggregates
# day
output_path_day = os.path.join( input_path.replace( 'hourly', 'daily' ) )
if not os.path.exists( output_path_day ):
	os.makedirs( output_path_day )
# month
output_path_mon = os.path.join( input_path.replace( 'hourly', 'monthly' ) )
if not os.path.exists( output_path_mon ):
	os.makedirs( output_path_mon )

# temp min -- daily
ds_min = ds.resample( 'D', dim='time', how='min' )
ds_min_comp = ds_min.compute()

# setup the output dataset with proper varname and attrs
new_varname = 'TMIN'
ds_min_comp = ds_min_comp[ variable.upper() ].to_dataset( name=new_varname )
ds_min_comp.attrs = global_attrs
ds_min_comp[ new_varname ].attrs = local_attrs
ds_min_comp[ 'lon' ].attrs = xy_attrs
ds_min_comp[ 'lat' ].attrs = xy_attrs

ds_min_comp.to_netcdf( os.path.join( output_path_day, '{}_wrf_day_{}.nc'.format( new_varname.lower(), group ) ), mode='w', format='NETCDF4_CLASSIC' )

# temp min -- monthly
ds_min_mon = ds_min_comp.resample( 'M', dim='time', how='mean' )
ds_min_mon_comp = ds_min_mon.compute()

# setup the output dataset with proper varname and attrs
# ds_min_comp[ variable.upper ].to_dataset( name=new_varname )
ds_min_mon_comp.attrs = global_attrs
ds_min_mon_comp[ new_varname ].attrs = local_attrs
ds_min_mon_comp[ 'lon' ].attrs = xy_attrs
ds_min_mon_comp[ 'lat' ].attrs = xy_attrs

ds_min_mon_comp.to_netcdf( os.path.join( output_path_mon, '{}_wrf_month_{}.nc'.format( new_varname.lower(), group ) ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
ds_min.close()
ds_min = None
ds_min_comp.close()
ds_min_comp = None
ds_min_mon_comp.close()
ds_min_mon_comp = None

# temp max -- daily
ds_max = ds.resample( 'D', dim='time', how='max' )
ds_max_comp = ds_max.compute()

# setup the output dataset with proper varname and attrs
new_varname = 'TMAX'
ds_max_comp = ds_max_comp[ variable.upper() ].to_dataset( name=new_varname )
ds_max_comp.attrs = global_attrs
ds_max_comp[ new_varname ].attrs = local_attrs
ds_max_comp[ 'lon' ].attrs = xy_attrs
ds_max_comp[ 'lat' ].attrs = xy_attrs

ds_max_comp.to_netcdf( os.path.join( output_path_day, '{}_wrf_day_{}.nc'.format( new_varname.lower(), group ) ), mode='w', format='NETCDF4_CLASSIC' )

# temp max -- monthly
ds_max_mon = ds_max_comp.resample( 'M', dim='time', how='mean' )
ds_max_mon_comp = ds_max_mon.compute()

# setup the output dataset with proper varname and attrs
# ds_min_comp[ variable.upper ].to_dataset( name=new_varname )
ds_max_mon_comp.attrs = global_attrs
ds_max_mon_comp[ new_varname ].attrs = local_attrs
ds_max_mon_comp[ 'lon' ].attrs = xy_attrs
ds_max_mon_comp[ 'lat' ].attrs = xy_attrs

ds_max_mon_comp.to_netcdf( os.path.join( output_path_mon, '{}_wrf_month_{}.nc'.format( new_varname.lower(), group ) ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
ds_max.close()
ds_max = None
ds_max_comp.close()
ds_max_comp = None
ds_max_mon_comp.close()
ds_max_mon_comp = None

# close master
ds.close()