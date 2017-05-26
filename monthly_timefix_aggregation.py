# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # MONTHLY DATA AGGREGATION and FIX TIME DIMENSION
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
if __name__ == '__main__':
	import os
	import xarray as xr
	import pandas as pd
	import numpy as np

	os.chdir( '/workspace/UA/malindgren/temporary' )
	# copied file from /storage01/pbieniek/erain/monthly to above temp folder

	# file cannot be properly decoded since it is a not standard time interval
	# accd to the UDUNITS pkg, months since and years since vary too much to 
	# be useful units of time measure and should therefore not be trusted to 
	# work properly...  I *think* if we had an explicit calendar we could 
	# figure it out, but I am unsure how to implement this in xarray
	# SEE: https://github.com/Unidata/netcdf4-python/issues/434
	ds = xr.open_dataset( 'monthly_TMAX-erai.nc', decode_times=False ) 
	variable = 'TMAX'

	# get undecoded timestring from the file and build a new timeseries of pd.Timestamp()'s
	timestr = ds.time.units
	year, month, day = timestr.split()[-2].split( '-' )
	new_dates = pd.date_range('-'.join([month, day, year]), periods=len( ds.time.data ), freq='M' )
	ds[ 'time' ] = new_dates # update the time variable in the NetCDF file as a workaround

	# subselect and convert it to Celcius from Kelvin
	tmax = ds[ variable ] - 273.15 # is this needed?

	# groupby and average monthlies for a given period:
	historical = ds.sel( time=slice('2006', '2016') ).groupby( 'time.month' ).mean( axis=0 )
	historical.to_netcdf( 'historical_TMAX_testing.nc' ) # this does not maintain the attr CF metadata...
