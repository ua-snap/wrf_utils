import xarray as xr
import numpy as np
import pandas as pd
import os

# setup pathing, etc.
input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

variable = 'pcpt'

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

# precip total -- daily
ds_total = ds.resample( 'D', dim='time', how='sum' )
ds_total_comp = ds_min.compute()
ds_total_comp.to_netcdf( os.path.join( output_path_day, 'PCPT_total_wrf_day.nc' ), mode='w', format='NETCDF4_CLASSIC' )

# precip total -- monthly
ds_total_mon = ds_total_comp.resample( 'M', dim='time', how='sum' )
ds_total_mon_comp = ds_total_mon.compute()
ds_total_comp.to_netcdf( os.path.join( output_path_mon, 'PCPT_total_wrf_month.nc' ), mode='w', format='NETCDF4_CLASSIC' )

