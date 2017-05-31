# gfdl rcp85 non-standard processing from the stored annually 
# stacked monthlies of multiple variables.

import xarray as xr
import numpy as np
import pandas as pd
import os, glob, itertools

input_path = '/storage01/pbieniek/gfdl/rcp85'
wildcard = '*_grcp85.nc'
files = glob.glob( os.path.join( input_path, wildcard ) )
output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf'
group = 'gfdl_rcp85'

variables = [ 'PCPT', 'TMAX', 'TMIN' ]

# open the mf_dataset
ds = xr.open_mfdataset( os.path.join(input_path, wildcard), autoclose=True )

# get some attrs
global_attrs = ds.attrs
xy_attrs = ds.lon.attrs
time_attrs = ds.time.attrs

# output the dailies in the new format.
for variable in variables:
	# get the variable attrs
	local_attrs = ds[ variable ].attrs

	# subset the dataset to the current variable and load it
	var_ds = ds[ variable ].to_dataset( name=variable ).compute()

	# set the attrs to the new ds
	var_ds.attrs = global_attrs
	var_ds['lon'].attrs = xy_attrs
	var_ds['lat'].attrs = xy_attrs
	var_ds['time'].attrs = time_attrs
	var_ds[ variable ].attrs = local_attrs

	# output the file to disk
	output_filename = os.path.join( output_path, 'daily', '{}_wrf_day_{}.nc'.format(variable, group) )]
	var_ds.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

	# now resample to monthlies
	metric = {'PCPT':'sum', 'TMAX':'mean', 'TMIN':'mean'}
	var_ds_mon = var_ds.resample( 'D', dim='time', how=metric[ variable ] )
	var_ds_mon_comp = var_ds_mon.compute()

	# to dataset
	var_ds_mon_comp = var_ds_mon_comp.to_dataset( name=variable )
	var_ds_mon_comp.attrs = global_attrs
	var_ds_mon_comp['lon'].attrs = xy_attrs
	var_ds_mon_comp['lat'].attrs = xy_attrs
	var_ds_mon_comp['time'].attrs = time_attrs
	var_ds_mon_comp[ variable ].attrs = local_attrs

	# output the file to disk
	output_filename = os.path.join( output_path, 'monthly', '{}_wrf_month_{}.nc'.format(variable, group) )]
	var_ds.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

	
