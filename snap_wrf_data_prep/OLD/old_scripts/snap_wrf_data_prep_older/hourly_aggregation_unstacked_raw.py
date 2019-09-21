# some work with the output wrf files in their more raw form where each file is 
# a single hour of a single day in a single year containing all(most?) of the variables
# generated.  This is not entirely useful from a SNAP standpoint, but for a data delivery we 
# need to compute the 3hour averages of these odd files and stack them in a more useful way.

# this script constitutes a first stab at working with these hourly outputs.
# expect much to be non-working and in need of cleanup.  Feel free to make changes and submit PR's
# I will accept any/all improvements... as long as they work.

import cf_units, os
import xarray as xr
import pandas as pd
import numpy as np

year = 1979
hourly_dir = '/storage01/pbieniek/erain/hourly'
files = glob.glob( os.path.join( hourly_dir, str(year), 'WRFDS_d01.{}*.nc'.format(str(year)) ) )

def get_month_day( fn ):
	dirname, basename = os.path.split( fn )
	year, month, day_hour = basename.split('.')[-2].split('-')
	day, hour = day_hour.split( '_' )
	folder_year = dirname.split('/')[-1]
	return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}

def open_group_mean( files, variable ):
	'''average across a group of files and return a 2D array of the mean data'''
	import xarray as xr
	import numpy as np
	return sum([ xr.open_dataset(fn)[variable] for fn in files ]) / float(len(files))

# make a dataframe of the output splitting and listing
df = pd.DataFrame([ get_month_day( fn ) for fn in files ])
# group, sort on hour, and grab it
files_grouped = [ i.sort_values('hour')['fn'].tolist() for j,i in df.groupby(['year', 'month', 'day']) ]
# split a single group of files into 3 hour interval groups...
# --> this will require a waltz through all of the files_grouped above...
day_3hr_grouped = [ list(i) for i in np.split( np.array( files_grouped[0] ), 24/3 )]

# next steps will be to open each of the hourly files, grab the VARIABLE name of interest,
# and finally average them into a new layer...
# this could then go as far as re-stacking the data into a NetCDF, copying some CF-metadata, and building 
# a new file with a 3 hourly time interval used instead of the hourly one for a single var, single day?
