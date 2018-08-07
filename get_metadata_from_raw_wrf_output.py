# method to generate the proper projection information from the wrf files
# using some modified code from the `salem` python package.  I couldnt get it to work without a
# bit of finessing so I am making a script that will return us what we want.
# 
# --> THIS SCRIPT DOES __NOT__ REQUIRE SALEM.
# # # # # # # #

def get_meta_from_wrf( ds ):
    """Get the WRF projection out of the file."""
    import pyproj, rasterio

    wgs84 = pyproj.Proj( '+units=m +proj=latlong +datum=WGS84' )
    pargs = dict()
    # get some metadata from the RAW WRF file we got from Peter.
    cen_lon = ds.CEN_LON
    cen_lat = ds.CEN_LAT
    dx = ds.DX
    dy = ds.DY
    pargs['lat_1'] = ds.TRUELAT1
    pargs['lat_2'] = ds.TRUELAT2
    pargs['lat_0'] = ds.MOAD_CEN_LAT
    pargs['lon_0'] = ds.STAND_LON
    pargs['center_lon'] = ds.CEN_LON
    proj_id = ds.MAP_PROJ

    # setup the projection information from the information in the raw file
    # Polar stereo
    p4 = '+proj=stere +lat_ts={lat_1} +lon_0={lon_0} +lat_0=90.0' \
         '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
    p4 = p4.format( **pargs )

    proj = pyproj.Proj(p4)
    if proj is None:
        raise RuntimeError('WRF proj not understood: {}'.format(p4))

    # get dims from xarray dataset
    nx = ds.dims['west_east']
    ny = ds.dims['south_north']

    meta = dict()
    # make grid in polar coordinate system of SNAP-WRF
    e, n = pyproj.transform(wgs84, proj, cen_lon, cen_lat)
    # [ NOTE ]: these are centroid x0,y0 values of the lower-left...
    x0 = -(nx-1) / 2. * dx + e  # DL corner
    y0 = -(ny-1) / 2. * dy + n  # DL corner

    # flip the origin (UPPER LEFT CENTROID)
    res = 20000 # meters
    y0_ulcen = y0 + ((ny-1)*dy)
    x0_ulcen = x0

    ulx_cen = np.arange( x0_ulcen, x0_ulcen + ((dx*nx)), step=res )
    uly_cen = np.arange( y0_ulcen, y0_ulcen + (-(dy*ny)), step=-res )

    xc, yc = np.meshgrid( ulx_cen, uly_cen )

    # upper left corner coordinate origin...  NOT CENTROID for the proper affine
    origin = (x0_ulcen-(res/2.0), y0_ulcen+(res/2.0))
    transform = rasterio.transform.from_origin( origin[0], origin[1], res, res )
    
    # build affine transform
    meta.update( resolution=(nx, ny), 
                origin=origin, 
                shape=(dx, dy),
                crs=proj,
                origin_corner='UPPER-LEFT', 
                xc=xc, yc=yc, 
                transform=transform )

    return meta


if __name__ == '__main__':
    import os
    import numpy as np
    import xarray as xr

    os.chdir( '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example' )
    ds = xr.open_dataset('wrfout_d01_2025-07-10_00:00:00')

    wrf_meta = get_meta_from_wrf( ds )
