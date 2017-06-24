# process smoke data from wrf-chem
# currently specific to the PM_2_5_DRY variable...
# author: Michael Lindgren (malindgren@alaska.edu)
#      Scenarios Network For Alaska + Arctic Planning

def process_timestamp( t ):
    ''' specific to the wrf smoke inputs '''
    ts = str( t.data ).strip('"b').strip( "'" )
    ts = ts.replace( '_', ' ' )
    pt = pd.Timestamp( ts )
    return {'timestamp_raw':ts,'year':pt.year, 'month':pt.month, 
            'day':pt.day, 'hour':pt.hour,
            'minute':pt.minute, 'second':pt.second }

if __name__ == '__main__':
    import rasterio, os
    from rasterio.warp import calculate_default_transform, Resampling, reproject
    from rasterio.transform import array_bounds
    import xarray as xr
    import pandas as pd
    import numpy as np

    # open the netcdf file
    variable = 'PM2_5_DRY'
    ds = xr.open_dataset( '/workspace/Shared/Users/malindgren/wrf_smoke/raw/wrfout_d01_2017-06-15_00:00:00' )
    output_path = '/workspace/Shared/Users/malindgren/wrf_smoke'

    # subset to the variable we want
    sub_ds = ds['PM2_5_DRY']

    # make the dataset somewhat sane
    lon = sub_ds[ 'XLONG' ][ 0 ]
    lat = sub_ds[ 'XLAT' ][ 0 ]

    # make the times something that are easily parseable
    time_raw = ds[ 'Times' ]
    df = pd.DataFrame( [process_timestamp( t ) for t in time_raw] )
    df = df.sort_values(['year', 'month', 'day', 'hour', 'minute', 'second'])
    start_date = df.iloc[0]
    # leverage pandas datetime functionality to make legit timestamps
    new_dates = pd.date_range( df.loc[0,'timestamp_raw'], periods=df.shape[0], freq='1H' )

    # make a NetCDF-like Dataset in memory 
    # NOTE: I have no idea what the level variable is here but I am including it
    # the data for the variable seems to be structured (time,?,x,y)
    out_ds = xr.Dataset( {variable:(['time','level','x', 'y'], sub_ds.data)},
                coords={'lon': (['x', 'y'], lon),
                        'lat': (['x', 'y'], lat),
                        'time': new_dates,
                        'level':list(range(sub_ds.shape[1]))},
                attrs={ 'variable':variable,
                        'wrf-chem':'smoke model',
                        'postprocessed by:':'Michael Lindgren -- SNAP'} )


    # write this to disk -- if you want...
    output_filename = os.path.join( output_path, 'netcdf', 'wrfout_d01_{}_2017-06-15_00-00-00.nc'.format( variable ) )
    
    # make output directory if needed
    dirname, basename = os.path.split( output_filename )
    if not os.path.exists( dirname ):
        os.makedirs( dirname )

    out_ds.to_netcdf( output_filename, 'w', 'NETCDF4_CLASSIC' )
    
    # * * * * * * * * * REPROJECTION: * * * * * * * * * * * * * * * * * * * * * * * * * * *
    # FROM THAT projectGDAL thingy:
    crs_proj4 = '+proj=lcc +lat_1=65 +lat_2=65 +lat_0=65 +lon_0=-152 +R=6370000'

    # FROM SALEM BASE CODE::
    crs_proj4 = '+units=m +proj=lcc +lat_1=65.0 +lat_2=65.0 +lat_0=65.0 +lon_0=-152.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    

    # HERE IS THE FINAL WAY TO DO THIS I THINK:
    from salem import sio
    
    # read our wrf output dataset in as a salem 'wrf' dataset. This is mainly used for metadata parsing.
    sds = sio._wrf_grid_from_dataset( ds )
    crs = rasterio.crs.CRS.from_string( sds.proj.srs )
    xmin, xmax, ymin, ymax = sds.extent

    affine = rasterio.transform.from_origin( xmin, ymax, sds.dx, sds.dy )
    time, levels, height, width = out_ds[ variable ].shape
    meta = {'res':(sds.dx, sds.dy), 'transform':affine, 'height':height, 'width':width, 'count':1, 'dtype':'float32', 'driver':'GTiff', 'compress':'lzw', 'crs':crs }


    # # some crs stuff that @bob got from the provider
    # crs_proj4 = "+proj=lcc +lat_1=65.000000 +lat_2=65.000000 +lat_0=65.000000 +lon_0=-152.000000 +R=6370000" # 
    # # crs_proj4 = "+proj=lcc +lat_1=65 +lat_0=72.0000 +lon_0=-152.000000 +R=6370000"
    # crs_proj4 = '+proj=lcc +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m no_defs'
    # # crs = "+proj=lcc +lat_0=70 +lat_1=70 +lon_0=-164.510605 +a=6367470 +b=6367470 +ellps=sphere +datum=WGS84"
    # # crs = "+proj=lcc +lat_0=40 +lat_1=20 +lat_2=60 +lon_0=-120 +ellps=sphere"
    # # crs = "+proj=lcc +lat_0=30 +lat_1=35 +lon_0=-152 +ellps=sphere" # old coords new proj4
    # # crs = "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    # crs = {'init':'epsg:4326'}
    # crs = rasterio.crs.CRS.from_string( crs_proj4 )
    # gcps = "-gcp 0 0 -164.510605 57.652809 -gcp 0 298 -172.422668 70.599640 -gcp 298 0 -139.489395 57.652809 -gcp 298 298 -131.577332 70.599640"
    # GCPS = [ [float(j) for j in i.split()] for i in gcps.split('-gcp ') if len(i) > 0 ]
    # GCPS = [[0, 298, -164.510605, 57.652809],
    #         [0, 0, -172.422668, 70.59964],
    #         [298, 298, -139.489395, 57.652809],
    #         [298, 0, -131.577332, 70.59964]]

    #         -te xmin ymin xmax ymax
            

    # gdal_translate -co COMPRESS=LZW -gcp 0 0 -164.510605 57.652809 -gcp 0 298 -172.422668 70.59964 -gcp 298 0 -139.489395 57.652809 -gcp 298 298 -131.577332 70.59964 /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest.tif /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_GCPS.tif
    # # gdalwarp -co "COMPRESS=LZW" -r bilinear -t_srs "+proj=lcc +lat_1=65 +lat_2=65 +lat_0=65 +lon_0=-152 +R=6370000" /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_GCPS.tif /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_CRS.tif
    # gdalwarp -overwrite -co "COMPRESS=LZW" -r bilinear -te -172.42266845703125 57.652809143066406 -131.57733154296875 71.68465423583984 -t_srs "+proj=lcc +lat_1=65 +lat_2=65 +lat_0=65 +lon_0=-152 +R=6370000" /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_GCPS.tif /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_CRS.tif
    # gdalwarp -overwrite -co "COMPRESS=LZW" -t_srs EPSG:3338 /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_CRS.tif /workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest_EPSG3338.tif

    # # ' '.join([ '-gcp '+' '.join([str(pixel), str(line), str(x1), str(y1)]) for pixel, line, x1, y1 in GCPS ])
    # gcps = [ GroundControlPoint( *gcp ) for gcp in GCPS ]
    # gcp_df = pd.DataFrame( GCPS, columns=['row','col','lon','lat'] )

    # # crs={'init':'epsg:4326'}


    # resolution is a real hacky way to do it and not correct
    # res = ( ((lon.max() - lon.min()) / 299).data, ((lat.max() - lat.min()) / 299).data )
    # res = ( 0.041675, 0.041675 ) # this is 1/4th the DD res of the 10-min data which is ~20km res... TOTAL HACK.
    # res = ( 5000.0, 5000.0 ) # incorrect but this is the res in meters...
    # # affine = rasterio.transform.from_origin( lon.data.min(), lat.data.max(), res[0], res[1] ) # upper left pixel
    # affine = rasterio.transform.from_origin( -747500.0, 747500.0, res[0], res[1] ) # A TEST!!! 
    # # affine = rasterio.transform.from_origin( -172.422668, 70.59963999, res[0], res[1] ) # upper left pixel
    # time, levels, height, width = out_ds[variable].shape
    # meta = {'res':res, 'transform':affine, 'height':height, 'width':width, 'count':1, 'dtype':'float32', 'driver':'GTiff', 'compress':'lzw', 'crs':crs }
    
    # # this is a temp test that will output the data in its 'raw' crs
    # with rasterio.open( '/workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw.tif', mode='w', **meta ) as out:
    #     out.write( np.flipud( out_ds_level[ 23, ... ].data ), 1 )

    # # # # TESTING SHIZ

    # def bounds_to_extent( bounds ):
    #     '''
    #     take input rasterio bounds object and return an extent
    #     '''
    #     l,b,r,t = bounds
    #     return [ (l,b), (r,b), (r,t), (l,t), (l,b) ]


    # bounds = [out_ds.lon.data.min(), out_ds.lat.data.min(), out_ds.lon.data.max(), out_ds.lat.data.max()]
    # minx, maxx = out_ds.lon.data.min(), out_ds.lon.data.max()
    # miny, maxy = out_ds.lat.data.min(), out_ds.lat.data.max()

    # from shapely.geometry import Polygon
    # import geopandas as gpd
    # # pol = Polygon([(minx, maxy),(maxx, maxy),(maxx,miny), (minx, miny),(minx, maxy)])
    # pol = Polygon( bounds_to_extent( bounds ) )
    # gdf = gpd.GeoDataFrame( {0:{'id':1,'geometry':pol}}, crs=crs_proj4 ).T
    # gdf.to_file( '/workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_extent.shp' )




    # # # # # # # # 
    # subset to one of the 29 'levels' no idea what these are...
    levelint = 0
    out_ds_level = out_ds[variable].isel( level=levelint )
    # meta2 = {'res':res, 'transform':affine, 'height':height, 'width':width, 'count':24, 'dtype':'float32', 'driver':'GTiff', 'compress':'lzw', 'crs':{'init':'epsg:4326'} }
    # with rasterio.open( '/workspace/Shared/Users/malindgren/wrf_smoke/smoke_raw_daytest.tif', mode='w', **meta2 ) as out2:
    #     out2.write( out_ds_level.data )

    # # warp to 3338
    src_bounds = array_bounds( width, height, affine )
    dst_crs = {'init':'epsg:3338'}
    
    # Calculate the ideal dimensions and transformation in the new crs
    dst_affine, dst_width, dst_height = calculate_default_transform(crs, dst_crs, width, height, *src_bounds)

    for band in range( time ):
        print( 'reprojecting: {}'.format(band) )
        # in/out arrays
        cur_arr = np.flipud( out_ds_level[ band, ... ].data )
        out_arr = np.empty_like( cur_arr )

        # reproject
        reproject( cur_arr, out_arr, src_transform=affine, src_crs=crs, src_nodata=-1,
                dst_transform=dst_affine, dst_crs=dst_crs, dst_nodata=-9999, resampling=Resampling.bilinear )
        
        meta_out = meta.copy()
        meta_out.update( crs=dst_crs, width=dst_width, height=dst_height, 
                                transform=dst_affine, compress='lzw', nodata=-9999 )
        try:
            _ = meta_out.pop('affine')
        except:
            pass
        
        # write out a GeoTiff
        row = df.iloc[band]
        output_filename = os.path.join( output_path, 'geotiff', 
            '{}_wrf-chem_{}_{}_{}-{}_level{}_epsg3338.tif'.format( variable,row['year'],row['month'],row['day'],row['hour'],levelint))

        # make output directory if needed
        dirname, basename = os.path.split( output_filename )
        if not os.path.exists( dirname ):
            os.makedirs( dirname )

        with rasterio.open( output_filename, 'w', **meta_out ) as out:
            out.write( out_arr, 1 )