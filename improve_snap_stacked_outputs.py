# make a independent lat/long version of the outputs using xarray -- impossible...

def make_salem_grid():
    ''' 
    function that will generate a salem GIS Grid from some 
    knowns about the RAW WRF outputs (not modified by AK-CSC)
    as derived from salem.open_wrf_dataset(). This was used to help
    figure out how the WRF outputs are stored, and a bunch of things
    related to spatial reference system and gridding.
    '''
    from salem.gis import Grid

    # [hardwired] wrf file metadata for the SNAP WRF data resources
    proj = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    nxny = (262,262) 
    dxdy = (20000.0,20000.0)
    x0y0 = (-2610000.0,-5402425.477371664)
    pixel_ref = 'center'
    return Grid( proj=proj, nxny=nxny, dxdy=dxdy, x0y0=x0y0, pixel_ref=pixel_ref )

def get_variable_name( ds ):
    ''' 
    simple function to grab the variable name from an open xarray dataset
    representing SNAP-modified WRF Dynamical Downscaling outputs for Alaska and 
    surrounding regions
    
    ARGUMENTS:
    ----------
    ds = [xarray.Dataset] open xarray object produced through SNAP's stacking processing.

    RETURNS:
    --------
    variable name as string

    '''
    variables = ds.variables.mapping.keys()
    dropkeys = ['time','lon','lat','longitude','latitude',
                'projection_x_coordinates','projection_y_coordinates',
                'x','y','levels','level']            
    variable, = [ variable for variable in variables if variable not in dropkeys ]
    return variable

def flip_data_and_coords( ds, variable=None ):
    ''' 
    flip each timestep of the series in an up/down direction
    the data are output from WRF with the southernmost portion of the map
    at the top and the northernmost at the bottom. This function will 
    flip the data values and the coordinate values.
    
    ARGUMENTS:
    ----------
    ds = [xarray.Dataset] open xarray object produced through SNAP's stacking processing.
    variable = [str] (optional) variable name to extract from the file.  Attempts will be made
                at grabbing the variable from the given file if it is not included.

    RETURNS:
    --------
    dict of flipped data and coordinate arrays
    structure of the returned dict: {'data':data,'x':x,'y':y}

    '''
    if variable is None:
        variable = get_variable_name( ds )

    dat = np.rollaxis( np.dstack([ np.flipud( ts ) for ts in ds[ variable ].values ]), -1)
    x = np.flipud( ds[ 'lon' ].values )
    y = np.flipud( ds[ 'lat' ].values )

    return { 'data':dat, 'x':x, 'y':y }

def make_variable_lookup( raw_fn ):
    # make a lookup table of variables and some metadata from the RAW WRF files.
    import xarray as xr

    # raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    raw = xr.open_dataset( raw_fn )

    return {i:{'long_name':raw[i].description, 'units':raw[i].units} for i in raw.variables.mapping.keys() if hasattr(raw[i], 'description') }

if __name__ == '__main__':
    import numpy as np
    import xarray as xr
    from collections import OrderedDict

    # input args...
    fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/test_proj/T2_wrf_hourly_gfdl_rcp85_2006.nc'
    out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/test_proj/T2_wrf_hourly_gfdl_rcp85_2006_IMPROVED.nc'

    # below is used for building some attrs into the files...
    raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    var_attrs_lookup = make_variable_lookup( raw_fn )

    # open the dataset to restructure / improve
    ds = xr.open_dataset( fn )

    # get the variable name from the already stacked files (produced by SNAP)
    variable = get_variable_name( ds )
    flipped = flip_data_and_coords( ds, variable=variable )

    # set up the grid to get the right origin and stuff
    # --> this should just be a way to grab the right origin, and build the 
    #     x/y coordinates using the array shape and cell resolution. 
    #     for now we rely on the wonderfully built and documented Salem package.
    grid = make_salem_grid()
    x,y = grid.xy_coordinates

    new_ds = xr.Dataset({variable: (['time','x', 'y'], flipped['data'])},
                        coords={'projection_x_coordinates': (['x', 'y'], np.flipud(x)),
                                'projection_y_coordinates': (['x', 'y'], np.flipud(y)),
                                'time': ds['time'].values })
    
    proj4string = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    
    # NATIVE FILE Attributes
    base_attrs = ds.attrs
    base_attrs.update( proj_parameters=proj4string, 
            restacked_by='Scenarios Network for Alaska + Arctic Planning -- November/December 2017' )
    new_ds.attrs = base_attrs

    # CRS Attributes
    crs_attrs = OrderedDict([
                    ('grid_mapping_name', 'polar_stereographic'),
                    ('straight_vertical_longitude_from_pole', -152.0),
                    ('latitude_of_projection_origin', 90.0),
                    ('standard_parallel', 64.0),
                    ('false_easting', 0),
                    ('false_northing', 0),
                    ('crs_wkt', 'PROJCS["unnamed",GEOGCS["unnamed ellipse",DATUM["unknown",SPHEROID["unnamed",6370000,0]],PRIMEM["Greenwich",0],\
                                UNIT["degree",0.0174532925199433]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",64],PARAMETER["central_meridian",-152],\
                                PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1],\
                                EXTENSION["PROJ4","+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +wktext"]]'),
                    ('proj_parameters', proj4string),
                    ('coordinate_location', 'centroid')
                    ])

    new_ds[ 'projection_x_coordinates' ].attrs = crs_attrs
    new_ds[ 'projection_y_coordinates' ].attrs = crs_attrs

    # VARIABLE attributes
    var_attrs = OrderedDict()
    var_attrs.update( long_name=var_attrs_lookup[ variable ][ 'long_name' ],
                       units=var_attrs_lookup[ variable ][ 'units' ],
                       coordinates_="projection_x_coordinates projection_y_coordinates"
                    )
    new_ds[ variable ].attrs = var_attrs

    # write it back out to disk with compression encoding
    encoding = new_ds[ variable ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    new_ds[ variable ].encoding = encoding

    # try:
    #     dirname = os.path.dirname( out_fn )
    #     if not os.path.exists( dirname ):
    #         os.makedirs( dirname )
    # except:
    #     pass

    new_ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )


