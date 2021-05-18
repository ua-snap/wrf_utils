# method to generate the proper projection information from the wrf files
# using some stolen code from the `salem` python package.  I couldnt get it to work without a
# bit of finessing so I am making a script that will return us what we want.
# 
# --> works with salem.__version__ == 0.2.1
# # # # # # # #

tmp_check_wrf = True # some hack he has in the source that is needed. seems harmless

def _wrf_grid_from_dataset(ds):
    """Get the WRF projection out of the file."""

    pargs = dict()
    if hasattr(ds, 'PROJ_ENVI_STRING'):
        # HAR and other TU Berlin files
        dx = ds.GRID_DX
        dy = ds.GRID_DY
        pargs['lat_1'] = ds.PROJ_STANDARD_PAR1
        pargs['lat_2'] = ds.PROJ_STANDARD_PAR2
        pargs['lat_0'] = ds.PROJ_CENTRAL_LAT
        pargs['lon_0'] = ds.PROJ_CENTRAL_LON
        pargs['center_lon'] = ds.PROJ_CENTRAL_LON
        if ds.PROJ_NAME in ['Lambert Conformal Conic',
                            'WRF Lambert Conformal']:
            proj_id = 1
        else:
            proj_id = 99  # pragma: no cover
    else:
        # Normal WRF file
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

    atol = 1e-4
    if proj_id == 1:
        # Lambert
        p4 = '+proj=lcc +lat_1={lat_1} +lat_2={lat_2} ' \
             '+lat_0={lat_0} +lon_0={lon_0} ' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
    elif proj_id == 2:
        # Polar stereo
        p4 = '+proj=stere +lat_ts={lat_1} +lon_0={lon_0} +lat_0=90.0' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
        # pyproj and WRF do not agree well close to the pole
        atol = 5e-3
    elif proj_id == 3:
        # Mercator
        p4 = '+proj=merc +lat_ts={lat_1} ' \
             '+lon_0={center_lon} ' \
             '+x_0=0 +y_0=0 +a=6370000 +b=6370000'
        p4 = p4.format(**pargs)
    else:
        raise NotImplementedError('WRF proj not implemented yet: '
                                  '{}'.format(proj_id))

    proj = gis.check_crs(p4)
    if proj is None:
        raise RuntimeError('WRF proj not understood: {}'.format(p4))

    # Here we have to accept xarray and netCDF4 datasets
    try:
        nx = len(ds.dimensions['west_east'])
        ny = len(ds.dimensions['south_north'])
    except AttributeError:
        # maybe an xarray dataset
        nx = ds.dims['west_east']
        ny = ds.dims['south_north']
    if hasattr(ds, 'PROJ_ENVI_STRING'):
        # HAR
        x0 = ds['west_east'][0]
        y0 = ds['south_north'][0]
    else:
        # Normal WRF file
        e, n = gis.transform_proj(wgs84, proj, cen_lon, cen_lat)
        x0 = -(nx-1) / 2. * dx + e  # DL corner
        y0 = -(ny-1) / 2. * dy + n  # DL corner
    grid = gis.Grid(nxny=(nx, ny), x0y0=(x0, y0), dxdy=(dx, dy), proj=proj)

    if tmp_check_wrf:
        #  Temporary asserts
        if 'XLONG' in ds.variables:
            # Normal WRF
            reflon = ds.variables['XLONG']
            reflat = ds.variables['XLAT']
        elif 'XLONG_M' in ds.variables:
            # geo_em
            reflon = ds.variables['XLONG_M']
            reflat = ds.variables['XLAT_M']
        elif 'lon' in ds.variables:
            # HAR
            reflon = ds.variables['lon']
            reflat = ds.variables['lat']
        else:
            raise RuntimeError("couldn't test for correct WRF lon-lat")

        if len(reflon.shape) == 3:
            reflon = reflon[0, :, :]
            reflat = reflat[0, :, :]
        mylon, mylat = grid.ll_coordinates
        np.testing.assert_allclose(reflon, mylon, atol=atol)
        np.testing.assert_allclose(reflat, mylat, atol=atol)

    return grid


if __name__ == '__main__':
    import xarray as xr
    import numpy as np
    from salem import gis, wgs84

    raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    ds = xr.open_dataset( raw_fn )

    grid = _wrf_grid_from_dataset( ds )

    # messing with the prepared data
    lons = grid.x_coord
    lats = grid.y_coord

    lons, lats = np.meshgrid( lons, lats )

    # read in one of the already stacked files...
    prep_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/test_proj/T2_wrf_hourly_gfdl_rcp85_2006.nc'
    ds = xr.open_dataset( prep_fn )

    # make new coordinates that we can use in indexing the grids beneath
    ds.coords['lon'] = (('x', 'y'), lons)
    ds.coords['lat'] = (('x', 'y'), lats)

