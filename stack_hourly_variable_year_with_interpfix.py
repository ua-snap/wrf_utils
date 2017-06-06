def nan_helper(y):
    '''
    Helper to handle indices and logical indices of NaNs.

    Input:
        - y, 1d numpy array with possible NaNs
    Output:
        - nans, logical indices of NaNs
        - index, a function, with signature indices= index(logical_indices),
          to convert logical indices of NaNs to 'equivalent' indices
    Example:
        >>> # linear interpolation of NaNs
        >>> nans, x= nan_helper(y)
        >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])
    '''
    return np.isnan(y), lambda z: z.nonzero()[0]
def interp_1d_along_axis( y ):
	''' interpolate across 1D timeslices of a 3D array. '''
	nans, x = nan_helper( y )
	y[nans] = np.interp( x(nans), x(~nans), y[~nans] )
	return y
def get_month_day( fn ):
	''' split WRF output filenames from UAF group and split to descriptor parts '''
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
def list_files( dirname ):
	'''
	list the files and split the filenames into their descriptor parts and 
	return dataframe of elements and filename sorted by:['year', 'month', 'day', 'hour']
	'''
	import os
	import pandas as pd

	files = [ get_month_day( os.path.join(r, fn)) for r,s,files in os.walk( dirname ) 
						for fn in files if os.path.split(r)[-1].isdigit() and fn.endswith( '.nc' ) and '-*.nc' not in fn]
	files_df = pd.DataFrame( files )
	return files_df.sort_values( ['year', 'month', 'day','hour'] ).reset_index()
def interp_file_grouper( df, reinit_day_value=6 ):
	''' return filenames for linear interpolation. format:(t-1, t, t+1) '''
	ind, = np.where( df.forecast_time == reinit_day_value )
	interp_list = [ df.iloc[[i-1,i,i+1]].fn.tolist() if i-1 > 0 
					else df.iloc[[i,i+1]].fn.tolist() for i in ind ]
	return interp_list
def stack_year( df, variable ):
	''' open and stack a single level dataset '''
	return np.array([ open_ds( fn, variable ) for fn in df.fn ])
def _interpolate_hour( x ):
	''' interpolate over a missing hour using hours on each side '''
	# open arrays
	arr = np.array([open_ds(fn, variable) for fn in x ])
	# fill second array with nan
	arr[1,...] = np.nan
	# interpolate
	out = np.apply_along_axis( 
		interp_1d_along_axis, axis=0, arr=arr.copy() )
	return out
def _update_arr( arr, key, value ):
	''' helper to update an array in a listcomp '''
	arr[ key, ... ] = value
	return arr
def run_year( df, variable ):
	import numpy as np
	DIFF_VARS = ['PCPT']

	ind, = np.where( df.interp == True )
	# stack the years hourly data
	arr = stack_year( df, variable )
	
	# do special stuff to the accumulation vars
	if variable in DIFF_VARS:
		# diff it and add back that lost timestep -- with a copy?
		diff_arr = np.diff( arr.copy() )
		diff_arr = np.append( diff_arr, arr[-1,...].copy(), axis=0 )
		arr = diff_arr.copy()
		del diff_arr
	
	# make the data needing interpolation all NaN
	interpolated = {count:interpolate_hour(i) for i, count in enumerate(df.interp_files) if len(i) == 3}
	# put the data back into the arr
	_ = [ update_arr( arr, key, value ) for key, value in interpolated.items() ]
	
	# Make sure we don't have any negative precip and set it to 0 if so (it happens) 
	if variable in DIFF_VARS:
		arr[ (arr < 0) | (arr == np.nan) ] = 0

	return arr

if __name__ == '__main__':
	import os
	import xarray as xr
	import numpy as np
	import pandas as pd

	# list files
	input_path = ''
	df = list_files( input_path )
	
	# set vars based on whether to interp
	ind, = np.where( df.forecast_time == 6 )
	df[ 'interp' ] = False
	df.iloc[ ind, np.where(df.columns == 'interp')[0] ] = True
	interp_list = interp_file_grouper( df )
	df[ 'interp_files' ] = [ [i] for i in df.fn ]
	df.iloc[ ind, np.where(df.columns == 'interp_files')[0] ] = interp_list

	# run year:
	year = 1980
	sub_df = df.loc[ df['year'] == year, ] 
	arr = run_year( df, variable )

	# build the output NetCDF Dataset -- FUNCTIONALIZE THIS
	new_dates = pd.date_range( '-'.join(sub_df[['month', 'day', 'year']].iloc[0]), periods=stacked_arr.shape[0], freq='1H' )

	# get some template data to get some vars from... -- HARDWIRED...
	mon_tmp_ds = xr.open_dataset( monthly_template_fn, decode_times=False )
	tmp_ds = xr.open_dataset( sub_df.fn.tolist()[0] )
	global_attrs = tmp_ds.attrs.copy()
	global_attrs[ 'reference_time' ] = str(new_dates[0]) # 1979 hourly does NOT start at day 01...  rather day 02....
	global_attrs[ 'proj_parameters' ] = "+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs"
	# global_attrs.pop()
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


	# write to disk
	



	



# ORDER OF OPERATIONS:
# -------------------
# - STACK YEAR


# - DIFF <-- PRECIP VARS only!
# - INTERP >> reading in adjacent years files if needed.
# - BUILD NETCDF

