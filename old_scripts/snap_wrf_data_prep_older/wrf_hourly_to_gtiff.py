# convert a band of the output NetCDF data from WRF to GTiff

if __name__ == '__main__':
	import xarray as xr
	import numpy as np
	import rasterio, os
	from rasterio.crs import CRS
	from affine import Affine

	# # stuff that will become CLI args...
	fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_daily_wrf_GFDL-CM3_rcp85_2012.nc'
	band = 1
	out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_testing_extent/wrf_extent_raw.tif'
	variable = 'T2'
	
	# potentially hardwire... for peters work at least.
	res = (20000.0,20000.0) # from the metadata in Peters outputs at the 'g5_lat_0' variable in the NC hourlies.

	# open the file
	ds = xr.open_dataset( fn )

	# get the affine transform for the output dataset
	# transform = transform_from_latlon( np.unique(ds.lat), np.unique(ds.lon) )
	# transform = transform_from_latlon( np.unique(ds.yc), np.unique(ds.xc) )
	# other way...  devised for the SMOKE outputs...
	# transform = rasterio.transform.from_origin( ds.lon.data.min(), ds.lat.data.min(), res[0], res[1] )
	transform = rasterio.transform.from_origin( ds.xc.data.min()-(res[0]/2.), ds.yc.data.max()+(res[0]/2.), res[0], res[1] )
	# nlayers, height, width = ds[ variable ].shape
	nlayers, height, width = ds[ variable.lower() ].shape
	
	# proj4string
	polar_proj = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'

	# make a metadata dict:
	meta = { 'transform':transform,
			'dtype':'float32',
			'count':1,
			'width':width,
			'height':height,
			'crs':rasterio.crs.CRS.from_string( polar_proj ),
			'driver':'GTiff',
			'compress':'lzw' }

	with rasterio.open( out_fn, 'w', **meta ) as out:
		out.write( ds[ variable.lower() ].isel( time=band ).astype( np.float32 ).data, 1 )

	print( 'completed: {}'.format( out_fn ) )

	
	# # # # # # # # # # # # # # # # # # # # # # # # # # # # 
	# # # # POLYGONIZE FOR EXTENT IN ANOTHER REFSYS # # # # 
	# # # # # # # # # # # # # # # # # # # # # # # # # # # # 
	
	# run a GDAL WARP with at rasterio raster made from the NetCDF slice. with the proper proj4string
	# canada_albers ='+proj=aea +lat_1=50 +lat_2=70 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m no_defs'
	os.system('cd /workspace/Shared/Tech_Projects/wrf_data/project_data/wrf')
	# os.system('gdalwarp -overwrite -t_srs EPSG:4326 -te -180 -90 180 90 -dstnodata -9999 wrf_extent_test.tif wrf_extent_4326.tif')
	os.system('gdalwarp -overwrite -multi -t_srs EPSG:3338 -tr 20000 20000 -dstnodata -9999 wrf_extent_raw.tif wrf_extent_3338.tif')

	# # ONE THING TO NOTE IS THAT IF WE PULL OUT THE EXTENT FROM THE RAW WRF TO A SHAPEFILE
	# #  REPROJECTION SHOULD OCCUR WITH ogr2ogr TO OVERCOME SOME ISSUES WITH THE BELOW POLYGONIZATION
	# ogr2ogr -overwrite -t_srs "EPSG:3338" wrf_extent_3338.shp wrf_extent_raw.shp	

	# now use rasterio to do the polygonizing:
	import rasterio, os
	from rasterio.features import shapes
	import numpy as np
	import geopandas as gpd
	from shapely.geometry import Polygon

	fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/wrf_extent_test_3338.tif'

	with rasterio.open( fn ) as rst:
		arr = rst.read( 1 )

	new_arr = (arr != -9999).astype(np.int16)
	out_shapes = [i for i in shapes(new_arr) if i[1] != 0]
	gdf = gpd.GeoDataFrame( {'id':list(range(len(out_shapes))),'geometry':[Polygon(shp[0]['coordinates'][0]) for shp in out_shapes]}, crs={'init':'EPSG:3338'}, geometry='geometry' )

	gdf.to_file('/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/wrf_extent_3338.shp')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # OLD NOTES:

# def transform_from_latlon( lat, lon ):
# 	''' simple way to make an affine transform from lats and lons coords '''
# 	from affine import Affine
# 	lat = np.asarray( lat )
# 	lon = np.asarray( lon )
# 	trans = Affine.translation(lon[0], lat[0])
# 	scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
# 	return trans * scale

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 