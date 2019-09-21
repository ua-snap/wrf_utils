# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # rasterize shape(s) to the extent/origin/res/crs of the SNAP WRF data.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

if __name__ == '__main__':
    import rasterio
    from rasterio.features import rasterize
    import numpy as np
    import xarray as xr
    import geopandas as gpd

    fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_hourly_wrf_ERA-Interim_historical_1979.nc'
    # shp_fn = '/workspace/UA/malindgren/repos/WRF_DOD/data/TEST_POINTS_WRF.shp'
    shp_fn = '/workspace/Shared/Tech_Projects/DOD_Ft_Wainwright/project_data/shapefiles/InstallationBoundary_SNAP_modified.shp'
    output_filename = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_testing_extent/wrf_rasterize_test_pols_alltouched.tif'
    variable = 't2'
    all_touched = True

    ds = xr.open_dataset( fn )
    da = ds[ variable ]
    
    shp = gpd.read_file( shp_fn ).to_crs( ds.proj_parameters )
    geoms = list(shp.geometry)
    
    res = 20000
    # origin = ds.xc.data.min()-(res/2.), ds.yc.data.max()+(res/2.)
    xc, yc = np.meshgrid( ds.xc.data, ds.yc.data )
    origin = xc[0,0]-(res/2.), yc[0,0]+(res/2.)
    x,y = origin
    transform = rasterio.transform.from_origin( x, y, res, res )

    # rasterize it
    mask = rasterize( geoms, out_shape=da[0].shape, fill=0, out=None, 
        transform=transform, all_touched=all_touched, default_value=1, dtype='float32' )

    to_gtiff( mask, origin, output_filename, nodata=-9999 )