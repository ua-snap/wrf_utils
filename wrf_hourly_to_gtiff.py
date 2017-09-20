# convert a band of the output NetCDF data from WRF to GTiff

def transform_from_latlon( lat, lon ):
	''' simple way to make an affine transform from lats and lons coords '''
	from affine import Affine
	lat = np.asarray( lat )
	lon = np.asarray( lon )
	trans = Affine.translation(lon[0], lat[0])
	scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
	return trans * scale


if __name__ == '__main__':
	import xarray as xr
	import numpy
	import numpy as np
	import rasterio
	from rasterio.crs import CRS
	from affine import Affine

	# # stuff that will become CLI args...
	fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/PCPT_wrf_hourly_gfdl_hist_1990_FULLTEST2.nc'
	band = 1
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/PCPT_wrf_hourly_gfdl_hist_1990_FULLTEST2_band{}_2.tif'.format(band)
	variable = 'PCPT'
	# potentially hardwire... for peters work at least.
	res = (19655.0,19655.0) # from the metadata in Peters outputs at the 'g5_lat_0' variable in the NC hourlies.

	# open the file
	ds = xr.open_dataset( fn )

	# get the affine transform for the output dataset
	transform = transform_from_latlon( np.unique(ds.lat), np.unique(ds.lon) )
	# other way...  devised for the SMOKE outputs...
	transform = rasterio.transform.from_origin( ds.lon.data.min(), ds.lat.data.min(), res[0], res[1] )
	nlayers, height, width = ds[ variable ].shape
	
	# proj4string -- built by hand from SALEM source... WATCH THIS.
	polar_proj = '+proj=stere +lat_ts={} +lat_0=90.0 +lon_0={} +x_0=0 +y_0=0 +a=6370000 +b=6370000'.format( 37.233, -177.786 )
	# polar_proj = '+proj=stere +lat_ts={} +lat_0=90.0 +lon_0={} +x_0=0 +y_0=0 +a=6370000 +b=6370000'.format( 64, -152 )
	# FROM RUDI via PETER:
	# polar_proj = '+proj=stere +lat_0=90 +lat_ts=64 +lon_0=-152 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
	# crs = CRS.from_string( polar_proj )
	
	# RUDI's AFFINE FROM GTiff...
	crs = Affine(19996.451606182643, 0.0, -2609990.449893133, 0.0, -19996.451606182643, -183320.66272092026)
	
	# make a metadata dict:
	meta = { 'transform':transform,
			'dtype':'float32',
			'count':1,
			'width':width,
			'height':height,
			'crs':crs,
			'driver':'GTiff',
			'compress':'lzw' }

	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(ds[ variable ].isel( time=band ).astype( np.float32 )), 1 )

	print( 'completed: {}'.format( out_fn ) )