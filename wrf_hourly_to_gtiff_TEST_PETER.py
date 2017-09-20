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
	# fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/PCPT_wrf_hourly_gfdl_hist_1990_FULLTEST2.nc'
	band = 12
	# out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/TEST_ACCUM/PCPT_TEST_INTERP_{}.tif'.format(band)
	# variable = 'PCPT'
	# potentially hardwire... for peters work at least.
	res = (19655.0,19655.0) # from the metadata in Peters outputs at the 'g5_lat_0' variable in the NC hourlies.

	# open the file
	# ds = xr.open_dataset( fn )

	# TEST FILENAMES:
	files = ['/storage01/pbieniek/gfdl/hist/hourly/1990/WRFDS_d01.1990-01-02_23.nc','/storage01/pbieniek/gfdl/hist/hourly/1990/WRFDS_d01.1990-01-03_00.nc']
	a,b = files
	Tminus1 = open_ds(a, variable)
	T = open_ds(b, variable)

	# READ RUDI's GTIFF FILE
	template_rst = rasterio.open( '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf-pixel-SEAK-rebecca/2013-12-24_erai_T2_fromRudi.tif' )
	meta = template_rst.meta.copy()
	meta.update( compress='lzw' )
	band = 'TEST_24Hours'
	
	# RAW PCPT
	# --> tminus1
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/TEST_ACCUM/WRFDS_d01_1990-01-02_23.tif'.format( band )
	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(Tminus1.astype(np.float32)), 1 )
	# --> t
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/TEST_ACCUM/WRFDS_d01_1990-01-03_00.tif'.format( band )
	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(T.astype(np.float32)), 1 )

	# DIFFED
	diff_arr = open_diff( files, variable )
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/TEST_ACCUM/DIFF_T_minus_Tminus1_1990.tif'.format( band )
	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(diff_arr.astype(np.float32)), 1 )


	def open_ds( fn, variable ):
	    ''' cleanly read variable/close a single hourly netcdf '''
	    import xarray as xr
	    ds = xr.open_dataset( fn, autoclose=True )
	    out = ds[ variable ].copy()
	    ds.close()
	    return out.data

	fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/pcpt/PCPT_wrf_hourly_gfdl_hist_1990.nc'
	arr = open_ds( fn, variable )

	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/PCPT_wrf_hourly_gfdl_hist_1990_TEST.tif'
	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(arr[0,...].astype(np.float32)), 1 )


	# # # #NOT YET# # # 

	# INTERPED
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/TEST_ACCUM/PCPT_TEST_INTERP_{}.tif'.format(band)
	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( np.flipud(arr[:24, ...].astype(np.float32)) )

	# get the affine transform for the output dataset
	# transform = transform_from_latlon( np.unique(ds.lat), np.unique(ds.lon) )
	# # other way...  devised for the SMOKE outputs...
	# transform = rasterio.transform.from_origin( ds.lon.data.min(), ds.lat.data.min(), res[0], res[1] )
	# RUDI's AFFINE FROM GTiff..
	transform = Affine(*[19996.451606182643, 0.0, -2609990.449893133, 0.0, -19996.451606182643, -183320.66272092026])
	
	nlayers, height, width = arr.shape
	
	# proj4string -- built by hand from SALEM source... WATCH THIS.
	polar_proj = '+proj=stere +lat_ts={} +lat_0=90.0 +lon_0={} +x_0=0 +y_0=0 +a=6370000 +b=6370000'.format( 37.233, -177.786 )
	# polar_proj = '+proj=stere +lat_ts={} +lat_0=90.0 +lon_0={} +x_0=0 +y_0=0 +a=6370000 +b=6370000'.format( 64, -152 )
	# FROM RUDI via PETER:
	# polar_proj = '+proj=stere +lat_0=90 +lat_ts=64 +lon_0=-152 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
	crs = CRS.from_string( polar_proj )
		
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