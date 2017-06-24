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

def restructure_wrf_variable( fn, variable='PM2_5_DRY' ):
    '''
    open and slice out a single variable from the wrf-smoke
    outputs.  Only currently tested on `PM2_5_DRY` 
    '''
    # open and subset variable
    ds = xr.open_dataset( fn )
    sub_ds = ds[ variable ]

    # make the dataset somewhat sane
    lon = sub_ds[ 'XLONG' ][ 0 ]
    lat = sub_ds[ 'XLAT' ][ 0 ]

    # make the times something that are easily parseable
    time_raw = ds[ 'Times' ]
    df = pd.DataFrame([ process_timestamp( t ) for t in time_raw ])
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
    return out_ds

def reproject_band( arr, src_transform, src_crs, src_nodata, dst_transform, dst_crs, dst_nodata, output_filename ):
    '''
    reproject array to destination crs

    simple wrapper around `rasterio.warp.reproject`
    '''
    # in/out arrays
    out_arr = np.empty_like( arr )

    # reproject
    reproject( arr, out_arr, src_transform=src_transform, src_crs=src_crs, src_nodata=src_nodata,
            dst_transform=dst_transform, dst_crs=dst_crs, dst_nodata=dst_nodata, resampling=Resampling.bilinear )

    meta_out = meta.copy()
    meta_out.update( crs=dst_crs, width=dst_width, height=dst_height, 
                            transform=dst_affine, nodata=dst_nodata, compress='lzw' )
        
    try: # if not using rasterio 1.0+ this will break the affine
        _ = meta_out.pop( 'affine' )
    except:
        pass

    # make output directory if needed
    dirname, basename = os.path.split( output_filename )
    if not os.path.exists( dirname ):
        os.makedirs( dirname )

    with rasterio.open( output_filename, 'w', **meta_out ) as out:
        out.write( out_arr, 1 )

    return output_filename

if __name__ == '__main__':
    import rasterio, os
    from rasterio.warp import calculate_default_transform, Resampling, reproject
    from rasterio.transform import array_bounds
    import xarray as xr
    import pandas as pd
    import numpy as np
    import salem
    from salem import sio
    import argparse

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

    # parse the commandline arguments
    parser = argparse.ArgumentParser( description='reproject wrf-chem smoke outputs for PM2_5_DRY variable to EPSG:3338' )
    parser.add_argument( "-fn", "--filename", action='store', dest='fn', type=str, help="path to the wrf-chem output file (.nc)" )
    parser.add_argument( "-o", "--output_path", action='store', dest='output_path', type=str, help="path to the directory where the 24 hourly geotiffs will be stored" )
    parser.add_argument( "-v", "--variable", action='store', dest='variable', type=str, const='PM2_5_DRY', help="name of variable to reproject from the input file. Only tested on PM2_5_DRY" )
    parser.add_argument( "-l", "--level", action='store', dest='level', type=int, nargs='?',const=0, help="level integer of altitude to reproject. default and reccommended is 0. closest to ground-level." )
    parser.add_argument( "-q", "--quiet", action='store', dest='quiet', type=bool, nargs='?',const=False, help="if True print nothing to screen, if False (default) print to screen" )

    # parse the CLI args
    args = parser.parse_args()
    fn = args.fn
    output_path = args.output_path
    variable = args.variable
    level = args.level
    quiet = args.quiet

    ds = restructure_wrf_variable( fn, variable=variable )

    # # write new NetCDF file to disk
    # dirname, basename = os.path.split( fn )
    # basename = os.path.splitext( basename )[0]
    # output_filename = os.path.join( output_path, 'netcdf', basename + '_{}.nc'.format( variable ) )
    # dirname, basename = os.path.split( output_filename )
    # if not os.path.exists( dirname ):
    #     os.makedirs( dirname )
    # ds.to_netcdf( output_filename, 'w', 'NETCDF4_CLASSIC' )

    # * * * * * * * * * REPROJECTION: * * * * * * * * * * * * * * * * * * * * * * * * * * * 
    
    # read our wrf output dataset in as a salem 'wrf' dataset. This is mainly used for metadata parsing.
    #   SALEM-derived proj4: 
    #     '+units=m +proj=lcc +lat_1=65.0 +lat_2=65.0 +lat_0=65.0 +lon_0=-152.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    #   salem converts the extent values to that of the odd lambert system used by WRF-CHEM so we can build
    #   a proper affine transform for the raw data.
    sds = sio._wrf_grid_from_dataset( xr.open_dataset( fn, autoclose=True ) )
    
    # set-up input and output file metadata
    crs = rasterio.crs.CRS.from_string( sds.proj.srs )
    xmin, xmax, ymin, ymax = sds.extent
    affine = rasterio.transform.from_origin( xmin, ymax, sds.dx, sds.dy )
    time, levels, height, width = ds[ variable ].shape
    meta = {'res':(sds.dx, sds.dy), 'transform':affine, 'height':height, 'width':width, 'count':1, 'dtype':'float32', 'driver':'GTiff', 'compress':'lzw', 'crs':crs }

    # # # # # # # # 
    # subset to one of the 29 'levels' --> these are altitude levels I have been told.
    #  We are grabbing the level closest to the ground.
    ds_level = ds[ variable ].isel( level=level )

    # # warp to 3338
    src_bounds = array_bounds( width, height, affine )
    dst_crs = {'init':'epsg:3338'}

    # Calculate new dimensions and transform in the new crs
    dst_affine, dst_width, dst_height = calculate_default_transform( crs, dst_crs, width, height, *src_bounds )

    # set up reproject args
    args = {'src_transform':affine, 'src_crs':crs, 
            'src_nodata':-1, 'dst_transform':dst_affine, 
            'dst_crs':dst_crs, 'dst_nodata':-9999 }

    # reproject and output to new GeoTiff
    for band in range( time ):
        if args.quiet == False:
            print( 'reprojecting band: {}'.format(band+1) )
        
        dirname, basename = os.path.split( fn )
        basename = os.path.splitext( basename )[0]
        output_filename = os.path.join( output_path, 'geotiff', basename.replace(':','_') + '_{}_level{}_hour{}.tif'.format( variable, level, str(band+1) ) )
        dirname, basename = os.path.split( output_filename )
        if not os.path.exists( dirname ):
            os.makedirs( dirname )

        args.update( arr=np.flipud( ds_level[ band, ... ].data ), output_filename=output_filename )
        
        reproject_band( **args )

# # # # # EXAMPLE RUN
# import os

# fn = '/workspace/Shared/Users/malindgren/wrf_smoke/raw/wrfout_d01_2017-06-15_00:00:00'
# output_path = '/workspace/Shared/Users/malindgren/wrf_smoke' # adds subdirs 'netcdf' and 'geotiff'
# variable = 'PM2_5_DRY'

# os.system( 'python3 wrf_smoke_process.py -- -fn {} -o {} -v {}'.format( fn, output_path, variable ) )

