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
	
	if variable in DIFF_VARS:
		# Make sure we don't have any negative precip and set it to 0 if so (it happens) 
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
	sub_df = df.loc[ df['year'] == year, ] 
	arr = run_year( df, variable )

	# build the output NetCDF Dataset

	# write to disk
	



	



# ORDER OF OPERATIONS:
# -------------------
# - STACK YEAR


# - DIFF <-- PRECIP VARS only!
# - INTERP >> reading in adjacent years files if needed.
# - BUILD NETCDF

