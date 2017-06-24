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
    import salem
    from salem import sio

    '''
    BUILT USING:
    ------------
        python 3.5.0

        third-party libraries
        ---------------------------
        name        |  __version__
        ------------ --------------
        'numpy'     : '1.13.0',
        'pandas'    : '0.20.1',
        'rasterio'  : '1.0a9',
        'salem'     : '0.2.1',
        'xarray'    : '0.9.5'


    '''

    # # easy way to get a dict of libraries and versions
    # {'rasterio':rasterio.__version__,
    # 'xarray':xr.__version__,
    # 'numpy':np.__version__,
    # 'pandas':pd.__version__,
    # 'salem':salem.__version__}

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
    # read our wrf output dataset in as a salem 'wrf' dataset. This is mainly used for metadata parsing.
    sds = sio._wrf_grid_from_dataset( ds )
    # SALEM-derived proj4
    # crs_proj4 = '+units=m +proj=lcc +lat_1=65.0 +lat_2=65.0 +lat_0=65.0 +lon_0=-152.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    crs = rasterio.crs.CRS.from_string( sds.proj.srs )
    xmin, xmax, ymin, ymax = sds.extent

    affine = rasterio.transform.from_origin( xmin, ymax, sds.dx, sds.dy )
    time, levels, height, width = out_ds[ variable ].shape
    meta = {'res':(sds.dx, sds.dy), 'transform':affine, 'height':height, 'width':width, 'count':1, 'dtype':'float32', 'driver':'GTiff', 'compress':'lzw', 'crs':crs }


    # # # # # # # # 
    # subset to one of the 29 'levels' --> these are altitude levels I have been told.
    #  We are grabbing the level closest to the ground.
    levelint = 0
    out_ds_level = out_ds[variable].isel( level=levelint )

    # # warp to 3338
    src_bounds = array_bounds( width, height, affine )
    dst_crs = {'init':'epsg:3338'}

    # Calculate the ideal dimensions and transformation in the new crs
    dst_affine, dst_width, dst_height = calculate_default_transform(crs, dst_crs, width, height, *src_bounds)

    for band in range( time ):
        print( 'reprojecting: {}'.format( band ) )
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