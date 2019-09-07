# resample hourly to monthly for data delivery
def make_monthly( fn, metric, dim='time' ):
	''' make monthly for individual years '''
	import xarray as xr
	ds = xr.open_dataset( fn )

	# get some attrs
	global_attrs = ds.attrs
	local_attrs = ds[ variable ].attrs
	xy_attrs = ds.lon.attrs
	time_attrs = ds.time.attrs

	ds_mon = ds.resample( 'M', dim='time', how=metric )
	ds_mon_comp = ds_mon.compute() # watch this one

	ds_mon_comp.attrs = global_attrs
	ds_mon_comp[ variable ].attrs = local_attrs
	ds_mon_comp[ 'lon' ].attrs = xy_attrs
	ds_mon_comp[ 'lat' ].attrs = xy_attrs
	ds_mon_comp[ 'time' ].attrs = time_attrs
	return ds_mon_comp

if __name__ == '__main__':
	import xarray as xr
	import dask
	import numpy as np
	import pandas as pd
	import os, glob, itertools
	import multiprocessing as mp
	from functools import partial

	base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf'
	wildcards = [('GFDL-CM3_historical','*_hist*.nc')]#, ('GFDL-CM3_rcp85', '*rcp85*'), ('ERA_Interim', '*erain*')]
	variables = [ os.path.basename(i).upper() for i in glob.glob( os.path.join(base_path, 'hourly', '*' ) ) if os.path.isdir( i ) and 'slurm' not in i ]
	variables = ['TSLB']  # RE-DO OF THIS VAR FOR PETER AS A MEAN not TOTAL

	years = { 'GFDL-CM3_historical':(1970, 2005)} #,'GFDL-CM3_rcp85':(2006, 2100),'ERA_Interim':(1979, 2015) }

	for group, wildcard in wildcards:
		begin, end = years[ group ]
		
		for variable in variables:
			print( 'running: {}'.format( variable ))

			# metric switch -- aggregation
			if variable in ['PCPT','pcpt', 'PCPC', 'pcpc' ,'PCPNC', 'pcpnc', 'ACSNOW', 'acsnow']:
				metric = 'sum'
			else:
				metric = 'mean'

			files = sorted( glob.glob( os.path.join( base_path, 'hourly', variable.lower(), wildcard ) ) )
			files = [fn for fn in files for i in range(begin, end+1) if str(i) in list(os.path.basename(fn).split('.')[0].split('_'))]
				
			mm = partial( make_monthly, metric=metric )
			pool = mp.Pool( 10 )
			out = pool.map( mm, files )
			pool.close()
			pool.join()

			ds_mon_comp = xr.concat( out, dim='time' )

			# dump to disk
			out_fn = os.path.join( os.path.join( base_path, 'monthly', variable.lower(), '_'.join([variable.lower(), 'wrf_compress', group, 'monthly','-'.join([str(begin), str(end)]), metric]) + '.nc'  ) )
			
		
			output_path = os.path.dirname( out_fn )

			if not os.path.exists( output_path ):
				os.makedirs( output_path )

			ds_mon_comp.to_netcdf( out_fn, mode='w', format='NETCDF4_CLASSIC' )