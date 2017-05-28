# stack the data from WRF outputs to single variable, single year, hourly timestep
# python3

def get_month_day( fn ):
	dirname, basename = os.path.split( fn )
	year, month, day_hour = basename.split('.')[-2].split('-')
	day, hour = day_hour.split( '_' )
	folder_year = dirname.split('/')[-1]
	return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}
def open_ds( fn, variable ):
	''' cleanly read variable/close a single hourly netcdf '''
	import xarray as xr
	ds = xr.open_dataset( fn )
	out = ds[ variable ].copy()
	ds.close()
	return out.data

if __name__ == '__main__':
	import os, glob, itertools
	import xarray as xr
	import pandas as pd
	import numpy as np
	from collections import OrderedDict

	# setup vars
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'
	os.chdir( output_path )
	variables = ['T2'] # 'PCPT',
	years = range( 1979, 2015+1 )
	input_dir = '/storage01/pbieniek/erain/hourly'
	monthly_template_fn = '/storage01/pbieniek/erain/monthly/monthly_TMAX-erai.nc' # [Hardwired]

	# list / sort the netcdf files
	files_df = pd.DataFrame([ get_month_day( os.path.join(r, fn)) for r,s,files in os.walk( input_dir ) 
							for fn in files if fn.endswith( '.nc' ) and len(r.split(os.path.sep)) >=5 
							and '-*.nc' not in fn and 'WRFDS_d01.' in fn ])

	files_df = files_df.sort_values( ['year', 'month', 'day'] )

	for variable, year in itertools.product( variables, years ):
		print( 'working on {} - {}'.format(year, variable) )
		# subset and stack the data 
		sub_df = files_df[ (files_df.year == str(year)) & (files_df.folder_year == str(year)) ]

		print( 'stacking files' )
		stacked_arr = np.array([ open_ds(fn, variable) for fn in sub_df.fn.tolist() ])
		new_dates = pd.date_range( '-'.join(sub_df[['month', 'day', 'year']].iloc[0]), periods=stacked_arr.shape[0], freq='1H' )

		# get some template data to get some vars from... -- HARDWIRED...
		mon_tmp_ds = xr.open_dataset( monthly_template_fn, decode_times=False )
		tmp_ds = xr.open_dataset( sub_df.fn.tolist()[0] )
		global_attrs = tmp_ds.attrs.copy()
		global_attrs[ 'reference_time' ] = str(new_dates[0]) # 1979 hourly does NOT start at day 01...  rather day 02....
		global_attrs[ 'proj_parameters' ] = "+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs"
		global_attrs.pop()
		local_attrs = tmp_ds[ variable ].attrs.copy()
		xy_attrs = mon_tmp_ds.lon.attrs.copy()

		# this is the way that you should add the attr to the file with a proj4string
		# // global attributes:
		#      :proj_parameters = "+proj=merc +lon_0=90W" ;

		# build a new Dataset with the stacked timesteps and some we extracted from the input Dataset
		ds = xr.Dataset( {variable:(['time','x', 'y'], stacked_arr)},
						coords={'lon': (['x', 'y'], tmp_ds.g5_lon_1.data),
								'lat': (['x', 'y'], tmp_ds.g5_lat_0.data),
								'time': new_dates},
						attrs=global_attrs )

		# set the local attrs for the given variable we are stacking
		ds[ variable ].attrs = local_attrs

		# set the lon/lat vars attrs with the existing attrs from the monthly dataset now...
		ds[ variable ].attrs = xy_attrs

		# write the new file to disk
		print( 'writing to disk' )
		try:
			final_path = os.path.join( output_path, variable )
			if not os.path.exists( final_path ):
				os.makedirs( final_path )
		except:
			pass

		output_filename = os.path.join( final_path, variable.lower(), '{}_wrf_hour_{}.nc'.format(variable, year) )
		ds.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )
