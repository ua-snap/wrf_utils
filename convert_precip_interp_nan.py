# quick-and-dirty method to do the precip interpolation and 
# conversion to hourly amounts.

def nan_helper(y):
    """Helper to handle indices and logical indices of NaNs.

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
    """
    return np.isnan(y), lambda z: z.nonzero()[0]

def interp_1d_along_axis( y ):
	''' interpolate across 1D timeslices of a 3D array. '''
	nans, x = nan_helper( y )
	y[nans] = np.interp( x(nans), x(~nans), y[~nans] )
	return y

if __name__ == '__main__':
	import numpy as np
	import pandas as pd
	import xarray as xr
	import os

	# get some data to play with
	os.chdir( '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/daily' )
	ds = xr.open_dataset( 'pcpt_total_wrf_day.nc' )
	attr_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/'
	variable = 'PCPT'

	# # read in the associated csv doc file with info on `forecast_time`
	# df = pd.read_csv( attr_fn, index_col=0 )
	# ft = df[ (df['variable'] == variable) ]
	# ft = ft.sort_values( )
	# ft = ft.forecast_time
	# ind = np.where( ft == 6 )

	# make a dummy set
	ft = [ i for i in range( 24 ) ] # dummy
	ft = pd.Series( ft ) # dummy
	ft[ [0,6,12] ] = 6 # dummy
	ind = np.where( ft == 6 ) # dummy

	# ***** BUILD A SMALL DUMMY DATASET ************************************
	# grab a small subset as some dummy data
	dat = ds.PCPT.isel( time=range(24) ).data
	# add a layer of np.nan (nodata) to the series to mimick missing hours
	nas = np.empty_like( dat[0] ) # use a slice of the dat as a guide
	nas[:] = np.nan # set all values=nodata
	# ***********************************************************************

	# # FROM PETER'S EMAIL:
	# ----------------------
	# I'm unsure the order here... Should we do the interpolation first, then do the diff? 
	# diff'ing with missing data is always a hairy thing and to be honest I am not sure how 
	# numpy handles this case. It *works*, meaning it does not error-out, but I will need to 
	# dig deeper to see what is happening in there...

	# 1) Subtract at each grid point: Time T - T-1  (gives the amount of precip/snow/potevap in each hour)
	dat_diff = np.diff( dat, axis=0 )

	# add the na's into a slice of dat
	dat_diff[ ind ] = nas

	# 2) We run the model in 54-hr chunks and we do not keep the 6-hr spinup so we have to estimate the first 
	# 	hour (00utc) on the first day of each new run cycle. We do that by linearly interpolating across 23utc of 
	# 	the previous day and 01utc of the current day.

	# [ML] I think the best way forward here is to add back in np.nan grids in locations where an hour is missing

	# get the nans, and the data for 1D interp through time of a 3D array (time, lon, lat)
	out = np.apply_along_axis( interp_1d_along_axis, axis=0, arr=dat_diff.copy() ) # this works

	# 3) Make sure we don't have any negative precip and set it to 0 if so (it happens)
	out[ (out < 0) | (out == np.nan) ] = 0

	# then we can pass this back to the NetCDF object read with xarray and write out a new NetCDF
	# [ insert file i/o here ]

	# THE ABOVE IS ALL SOMETHING THAT SHOULD BE DONE IN THE STACKING PROCEDURE.  WE NEED TO GET TO A REINIT 
	# VARIABLE THAT WILL TELL US WHEN WE NEED TO INTERPOLATE SOME HOURS.  



# NOTES:
# INSPIRATION: https://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array

