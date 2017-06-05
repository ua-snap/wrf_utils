import xarray as xr
import numpy as np
import pandas as pd
import os

# setup pathing, etc.
input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

variable = 'pcpt'
group = 'erain'

# read in mfdataset
ds = xr.open_mfdataset( os.path.join( input_path, variable, '*'.join(['', variable.upper(), group, '.nc']) ) )

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

# precip total -- daily
ds_total = ds.resample( 'D', dim='time', how='sum' )
ds_total_comp = ds_total.compute()

# setup the output dataset with proper varname and attrs
# ds_total_comp[ variable.upper() ].to_dataset( name=variable.upper() )
ds_total_comp.attrs = global_attrs
ds_total_comp[ variable.upper() ].attrs = local_attrs
ds_total_comp[ 'lon' ].attrs = xy_attrs
ds_total_comp[ 'lat' ].attrs = xy_attrs

ds_total_comp.to_netcdf( os.path.join( output_path_day, '{}_total_wrf_day_{}.nc'.format(variable, group) ), mode='w', format='NETCDF4_CLASSIC' )

# precip total -- monthly
ds_total_mon = ds_total_comp.resample( 'M', dim='time', how='sum' )
ds_total_mon_comp = ds_total_mon.compute()

# setup the output dataset with proper varname and attrs
# ds_total_mon_comp[ variable.upper() ].to_dataset( name=variable.upper() )
ds_total_mon_comp.attrs = global_attrs
ds_total_mon_comp[ variable.upper() ].attrs = local_attrs
ds_total_mon_comp[ 'lon' ].attrs = xy_attrs
ds_total_mon_comp[ 'lat' ].attrs = xy_attrs

ds_total_mon_comp.to_netcdf( os.path.join( output_path_mon, '{}_total_wrf_month_{}.nc'.format(variable, group) ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
ds_total.close()
ds_total = None
ds_total_comp.close()
ds_total_comp = None
ds_total_mon.close()
ds_total_mon = None
ds_total_mon_comp.close()
ds_total_mon_comp = None

# close master
ds.close()