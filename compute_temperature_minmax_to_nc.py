import xarray as xr
import numpy as np
import pandas as pd
import os

# setup pathing, etc.
input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

variable = 't2'

# read in mfdataset
ds = xr.open_mfdataset( os.path.join( input_path, variable, '*'.join(['', variable.upper(), '.nc']) ) )

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
ds_min_comp.to_netcdf( os.path.join( output_path_day, 'T2_min_wrf_day.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# temp min -- monthly
ds_min_mon = ds_min_comp.resample( 'M', dim='time', how='mean' )
ds_min_mon_comp = ds_min_mon.compute()
ds_min_mon_comp.to_netcdf( os.path.join( output_path_mon, 'T2_max_wrf_month.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
ds_min.close(); 
ds_min = None
ds_min_comp.close(); 
ds_min_comp = None
ds_min_mon_comp = None

# temp max -- daily
ds_max = ds.resample( 'D', dim='time', how='max' )
ds_max_comp = ds_min.compute()
ds_max_comp.to_netcdf( os.path.join( output_path_day, 'T2_max_wrf_day.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# temp max -- monthly
ds_max_mon = ds_max_comp.resample( 'M', dim='time', how='mean' )
ds_max_mon_comp = ds_max_mon.compute()
ds_max_mon_comp.to_netcdf( os.path.join( output_path_mon, 'T2_max_wrf_month.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
ds_max.close(); 
ds_max = None
ds_max_comp.close(); 
ds_max_comp = None
ds_max_mon_comp.close(); 
ds_max_mon_comp = None

# close master
ds.close()

