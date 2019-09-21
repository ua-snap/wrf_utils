# # # # # # # # # # # # # # # # # # # # # # # # 
# TEST THE VERSIONS BETWEEN MINE AND PETERS
# # # # # # # # # # # # # # # # # # # # # # # # 

def transform_from_latlon( lat, lon ):
	''' simple way to make an affine transform from lats and lons coords '''
	from affine import Affine
	lat = np.asarray( lat )
	lon = np.asarray( lon )
	trans = Affine.translation(lon[0], lat[0])
	scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
	return trans * scale

def write_to_disk( arr, out_fn, lat, lon, index=0 ):
	''' write one of the wrf layers to disk '''
	import rasterio
	
	height, width = arr.shape[-2:]
	crs = {'init':'epsg:4326'}
	driver = 'GTiff'
	transform = transform_from_latlon( np.unique(lat), np.unique(lon) )

	meta = dict()
	meta.update( height=height, width=width, count=12, crs=None, 
				driver=driver, dtype='float32', transform=transform )

	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( arr[-12:, ...].astype(np.float32) )

	return out_fn


if __name__ == '__main__':
	import numpy as np
	import xarray as xr

	index = 2
	new = xr.open_dataset( '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/t2/t2_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc' )
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly_testing/NEW_T2_GFDLH_{}.tif'.format(index)
	write_to_disk( new['T2'].data, out_fn, new.lat.data, new.lon.data, index=index )

	old = xr.open_dataset( '/storage01/pbieniek/gfdl/hist/monthly/monthly_T2-gfdlh.nc', decode_times=False )
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly_testing/OLD_T2_GFDLH_{}.tif'.format(index)
	write_to_disk( new['T2'].data, out_fn, old.lat.data, old.lon.data, index=index )



	# diff = new['T2'].data - old['T2'].data

	# diff[ np.where(diff <= -1 ) ]



	# FIND THE LOCATIONS IN THE DATA WHERE THERE ARE DISCREPANCIES OR EXAMINE THE DATA IN QGIS.


	# where is peters data stored...

	new = xr.open_dataset( '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/pcpt/pcpt_wrf_GFDL-CM3_historical_monthly_1970-2005_sum.nc' )
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly_testing/NEW_PCPT_GFDLH_{}.tif'.format(index)
	write_to_disk( new['PCPT'].data, out_fn, new.lat.data, new.lon.data, index=index )

	old = xr.open_dataset( '/storage01/pbieniek/gfdl/hist/monthly/monthly_PCPT-gfdlh.nc', decode_times=False )
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly_testing/OLD_PCPT_GFDLH_{}.tif'.format(index)
	write_to_disk( old['PCPT'].data, out_fn, old.lat.data, old.lon.data, index=index )


	# diff = new['PCPT'].data - old['PCPT'].data

	# lat = new.lat.data
	# lon = new.lon.data

