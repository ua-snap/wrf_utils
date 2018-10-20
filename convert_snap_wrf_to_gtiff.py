# # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # convert a wrf netcdf layer(s) to GeoTiff...
# # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def to_gtiff( arr, origin, output_filename, nodata=-9999 ):
    import rasterio
    import numpy as np

    # set up args.  Some hardwired for our WRF data...
    x,y = origin
    res = 20000
    transform = rasterio.transform.from_origin( x, y, res, res )
    wrf_polar_crs = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'                
    dtype = 'float32'
    
    if arr.ndim == 2:
        height, width = arr.shape
        count = 1
    elif arr.ndim == 3:
        count, height, width = arr.shape
    else:
        ValueError( 'wrong number of array dimensions -- must be 2 or 3' )

    meta = {'compress':'lzw', 
            'count':count,
            'crs':wrf_polar_crs,
            'height':height,
            'width':width,
            'transform':transform,
            'dtype':dtype,
            'driver':'GTiff', 
            'nodata':nodata }

    with rasterio.open( output_filename, 'w', **meta ) as out:
        if arr.ndim == 2:
            out.write( arr.astype( np.float32 ), 1 )
        elif arr.ndim == 3:
            out.write( arr.astype( np.float32 ) )
    
    return output_filename

if __name__ == '__main__':
    import xarray as xr
    
    fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_hourly_wrf_ERA-Interim_historical_1980.nc'
    output_filename = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_testing_extent/wrf_extent_raw_TEST.tif'
    
    ds = xr.open_dataset( fn, autoclose=True )
    res = 20000
    origin = ds.xc.data.min()-(res/2.), ds.yc.data.max()+(res/2.)

    arr = ds.t2.data[0,...]
    to_gtiff( arr, origin, output_filename, nodata=-9999 )
    