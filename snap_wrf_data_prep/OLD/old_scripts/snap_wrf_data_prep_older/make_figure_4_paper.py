# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#     make the figure 4/5 from Hajo and Marks paper.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt
import xarray as xr
import geopandas as gpd
from affine import Affine
import numpy as np
import rasterio, os

# data
# netcdf_fn = '/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/nsidc_0051/NetCDF/nsidc_0051_sic_nasateam_1978-2017_Alaska.nc'
netcdf_fn = '/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/nsidc_0051/NetCDF/nsidc_0051_sic_nasateam_1978-2017_Alaska_hann_11.nc'
ds = xr.open_dataset( netcdf_fn )
a = Affine(*eval( ds.affine_transform )[:6]) # make an affine transform for lookups

# make barrow points and get their row/col locs
points_fn = '/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/selection_points/Barrow_points_bestfit.shp'
points = gpd.read_file( points_fn ).geometry.apply(lambda x: (x.x, x.y)).tolist()
colrows = [ ~a*pt for pt in points ]
colrows = [ (int(c),int(r)) for c,r in colrows ]
cols = [ c for r,c in colrows ]
rows = [ r for r,c in colrows ]

# make a climatology
clim_fn = netcdf_fn.replace( '.nc', '_climatology.nc' )
if not os.path.exists( clim_fn ):
	clim_sel = ds.sel( time=slice('1978','2013') )
	clim = clim_sel.groupby('time.dayofyear').mean('time')
	clim.to_netcdf( clim_fn )
else:
	clim = xr.open_dataset( clim_fn )

clim_vals_pts = [ clim.sic[:,r,c].values for c,r in colrows ]
clim_vals_mean = np.mean( clim_vals_pts, axis=0 )
clim_new = np.concatenate( (clim_vals_mean, clim_vals_mean) ) # to extend the series properly

for sl in [slice('1997-09','1998-10'), slice('2005-09','2006-10')]:
	ds_sel = ds.sel( time=sl )
	hold = [ ds_sel.sic[:,r,c].values for c,r in colrows ]
	annual_dat = np.mean(hold, axis=0)

	os.chdir('/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/PNG')
	with rasterio.open('freezeup_start_avg_allyears_ordinal.tif') as rst:
		freezeup_begin = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	with rasterio.open('freezeup_end_avg_allyears_ordinal.tif') as rst:
		freezeup_end = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)

	with rasterio.open('breakup_start_avg_allyears_ordinal.tif') as rst:
		breakup_begin = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	with rasterio.open('breakup_end_avg_allyears_ordinal.tif') as rst:
		breakup_end = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	fubu_dat = np.empty_like( annual_dat )
	fubu_dat[:] = np.nan

	times = ds_sel.time.to_index()
	ordinals = np.array([ int(x.strftime('%j')) for x in times ])

	ind = [ np.where( ordinals == x )[0][0] for x in [freezeup_begin,freezeup_end,breakup_begin,breakup_end] ]
	fubu_dat[ ind ] = annual_dat[ind]

	# clim
	os.chdir('/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/PNG')
	with rasterio.open('freezeup_start_avg_allyears_ordinal_climatology.tif') as rst:
		freezeup_begin = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	with rasterio.open('freezeup_end_avg_allyears_ordinal_climatology.tif') as rst:
		freezeup_end = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)

	with rasterio.open('breakup_start_avg_allyears_ordinal_climatology.tif') as rst:
		breakup_begin = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	with rasterio.open('breakup_end_avg_allyears_ordinal_climatology.tif') as rst:
		breakup_end = np.nanmean(rst.read(1)[rows,cols]).round(0).astype(int)
	
	fubu_clim = np.empty_like(clim_vals_mean)
	fubu_clim[:] = np.nan
	fubu_clim[ [freezeup_begin,freezeup_end,breakup_begin,breakup_end] ] = clim_vals_mean[[ freezeup_begin,freezeup_end,breakup_begin,breakup_end ]]

	# plot the 'annual' data
	plt.plot( annual_dat )

	# plot extended climatology
	plt.plot( clim_new[244:-122] )
	plt.plot( np.concatenate([fubu_clim,fubu_clim])[244:-122], 'bo' )
	plt.plot( fubu_dat, 'ro')

	plt.tight_layout()
	plt.savefig('/workspace/Shared/Tech_Projects/SeaIce_NOAA_Indicators/project_data/selection_points/barrow_avg_{}-{}.png'.format(sl.start.split('-')[0],sl.stop.split('-')[0]), figsize=(16,9), dpi=300)
	plt.cla()
	plt.close()
