import xarray as xr
import numpy as np
import pandas as pd
import os

# setup pathing, etc.
input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

variable = 't2'

# read in mfdataset
ds = xr.open_mfdataset( os.path.join( input_path, variable, '*'.join(['', variable.upper(), '.nc']) ) )

# convert to Daily Averages
output_path = os.path.join( input_path.replace( 'hourly', 'daily' ) )

# temp min
ds_min = ds.resample( 'D', dim='time', how='min' )
ds_min_comp = ds_min.compute()
ds_min.to_netcdf( os.path.join( output_path, 'T2_min_wrf_day.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# temp max
ds_max = ds.resample( 'D', dim='time', how='max' )
ds_max_comp = ds_min.compute()
ds_max.to_netcdf( os.path.join( output_path, 'T2_max_wrf_day.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# cleanup...
del ds

# convert Dailies to Monthlies

# temp min
ds_min_mon = ds_min.resample( 'M', dim='time', how='mean' )
ds_min_mon_comp = ds_min_mon.compute()
ds_min_mon_comp.to_netcdf( os.path.join( output_path, 'T2_max_wrf_month.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# temp max
ds_max_mon = ds_max.resample( 'M', dim='time', how='mean' )
ds_max_mon_comp = ds_max_mon.compute()
ds_min_mon_comp.to_netcdf( os.path.join( output_path, 'T2_max_wrf_month.nc' ), mode='w', format='NETCDF4_CLASSIC' )

