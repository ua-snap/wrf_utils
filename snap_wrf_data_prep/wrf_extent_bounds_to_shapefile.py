# # # # # # # # # # # #
# RASTERIO BOUNDS TO EXTENT SHAPEFILE
# # # # # # # # # # # # 

def bounds_to_extent( bounds ):
	'''
	take input list of bounds and return an extent list of 
	coordinate pairs. Can be used to generate a shapely Polygon.
	'''
	l,b,r,t = bounds
	return [ (l,b), (r,b), (r,t), (l,t), (l,b) ]
def extent_to_polygon( extent ):
	from shapely.geometry import Polygon
	return Polygon( extent )
def extent_to_shapefile( fn, output_filename ):
	''' 
	convert rasterio-readable raster extent to a Polygon
	shapefile.
	ARGUMENTS:
	----------
	fn = raster path
	output_filename = shapefile path to create
	RETURNS:
	--------
	path to new shapefile
	'''	
	import rasterio, os
	import geopandas as gpd

	# get poly
	rst = rasterio.open( fn )
	ext = bounds_to_extent( rst.bounds )
	pol = extent_to_polygon( ext )

	# make shapefile
	df = gpd.GeoDataFrame.from_dict( { 1:{ 'id':1, 'geometry':pol } }, orient='index' )
	gdf = gpd.GeoDataFrame( df, crs=rst.crs, geometry='geometry' )

	# cleanup
	if os.path.exists( output_filename ):
		os.unlink( output_filename )

	gdf.to_file( output_filename )
	return output_filename

if __name__ == '__main__':
	import rasterio

	fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/wrf_extent_raw.tif'
	output_filename = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/wrf_extent_raw.shp'

	with rasterio.open( fn ) as rst:
		bounds = rst.bounds
	
	out_shape = extent_to_shapefile( fn, output_filename )

