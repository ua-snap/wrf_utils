# IMPROVE THE SNAP OUTPUTS FOR DISTRIBUTION
# THIS IS A LIVING AND CHANGING SCRIPT AS I LEARN MORE ABOUT THE FILE STRUCTURE
# AND CF-CONVENTIONS AND FROM OTHERS NEEDS, I AM UPDATING THE FILE METADATA AND 
# COORDS / DIMS APPROPRIATELY. 
# 
# June 2018 - Michael Lindgren (malindgren@alaska.edu)
# # # # # # # # # # # # # # # # # # # # # 

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
                'xc','yc',
                'x','y','levels','level','lv_DBLY3','lv_ISBL2']
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

    data = np.array( ds[ variable ] )
    x = np.flipud( ds[ 'lon' ].values )
    y = np.flipud( ds[ 'lat' ].values )

    if len(data.shape) == 3:
        dat = np.rollaxis( np.dstack([ np.flipud( ts ) for ts in data ]), -1)
        out = { 'data':dat, 'x':x, 'y':y }
    elif len(data.shape) == 4:
        dat = np.stack([ np.flip(ts, axis=1) for ts in data ])
        out = { 'data':dat, 'x':x, 'y':y }
    return out

def make_variable_lookup( raw_fn ):
    # make a lookup table of variables and some metadata from the RAW WRF files.
    import xarray as xr

    # raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    raw = xr.open_dataset( raw_fn )
    dat = {i:{'long_name':raw[i].long_name, 'units':raw[i].units} for i in raw.variables.mapping.keys() }
    # update missing variables from the list as we have found them....
    dat.update( PCPT={'long_name': 'Total precipitation', 'units':'mm'},
                QBOT={'long_name': 'Specific humidity at lowest model level', 'units':'kg/kg'} )
    return dat

def filelister( base_dir ):
    return [ os.path.join(r,fn) for r,s,files in os.walk( base_dir ) 
                                for fn in files if fn.endswith('.nc') ]

def force_update_times_UTC( fn ):
    ''' use the base netCDF4 python pkg to see if we can add UTC to times.'''
    import netCDF4 as nc4

    # open the dataset with xarray to cut down on code
    with xr.open_dataset( fn ) as ds:
        dates = ds.time.to_index().to_pydatetime().tolist()

    ds = None

    f = nc4.Dataset( fn, mode='r+' )
    units = f['time'].units
    suffix = ' UTC'
    if not units.endswith( suffix ):
        units = units + suffix

    # units = 'hours since 1979-01-02 00:00 UTC'
    values = nc4.date2num(dates, units=units)
    f['time'][:] = values
    f['time'].units = units

    f.close()
    return fn 

def run( fn ):
    print( fn )

    # make an output name from the input name.
    out_fn = fn.replace( '/hourly','/hourly_fix' )
    dirname, basename = os.path.split( out_fn )
    basename, ext = os.path.splitext( basename )

    # [NOTE]: naming below is hardwired to a naming that is identical (element-wise) 
    #    to this: 'ACSNOW_wrf_hourly_gfdl_rcp85_2018' (.nc removed above)
    variable, group, timestep, model, scenario, year = basename.split( '_' )
    modelnames = {'gfdl':'GFDL-CM3','era':'ERA-Interim'}
    scenarionames = {'rcp85':'rcp85', 'hist':'historical','interim':'historical'}
    # metric_lookup = {'min':'Minimum', 'max':'Maximum', 'sum':'Sum/Total', 'mean':'Mean/Avg'}

    basename = '_'.join([variable.lower(),timestep,group,modelnames[model],scenarionames[scenario],year]) + ext
    out_fn = os.path.join( dirname, basename )

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
    time = ds['time'] #.values
    base_attrs = ds.attrs.copy()

    # update time attributes.
    time_attrs = time.attrs.copy()
    time_attrs.update( {'time zone': 'all times are UTC.'})
    time.attrs = time_attrs

    if len(flipped['data'].shape) == 3:
        # build a new NetCDF and dump to disk -- with compression
        new_ds = xr.Dataset({variable.lower(): (['time','yc', 'xc'], flipped['data'])},
                            coords={'xc': ('xc', np.flipud(x)[0,]),
                                    'yc': ('yc', np.flipud(y)[:,0]),
                                    'lon':(['yc','xc'], np.flipud(ds.lon.data) ),
                                    'lat':(['yc','xc'], np.flipud(ds.lat.data) ),
                                    'time':time })
        level_attrs = None
        
    elif len(flipped['data'].shape) == 4: #(time,levels, x, y )
        # levelname to use if 4D
        level_attrs = None
        if variable in ['TSLB','SMOIS','SH2O']:
            levelname = 'lv_DBLY3'
        else:
            levelname = 'lv_ISBL2'
        
        level_attrs = var_attrs_lookup[levelname]

        # [NEW] make a new level name...
        levels_lu = {'lv_ISBL2':'plev', 'lv_DBLY3':'depth'}
        leveldata = ds[levelname].values
        levels = xr.DataArray( leveldata, coords={'plev':leveldata}, dims=['plev'] )
        # [NEW] end make a new level name

        # build dataset with levels at each timestep
        new_ds = xr.Dataset( {variable.lower():(['time',levelname,'yc', 'xc'], flipped['data'])},
                    coords={'xc': ('xc', np.flipud(x)[0,]),
                            'yc': ('yc', np.flipud(y)[:,0]),
                            'lon':(['yc','xc'], np.flipud(ds.lon.data) ),
                            'lat':(['yc','xc'], np.flipud(ds.lat.data) ),
                            'time': time,
                            'levels':levels})
    else:
        raise BaseException( 'wrong number of dimensions' )

    # update some file attrs...
    proj4string = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
    
    # native file Attributes
    base_attrs.update( proj_parameters=proj4string, 
        crs_wkt='PROJCS["unnamed",GEOGCS["unnamed ellipse",DATUM["unknown",SPHEROID["unnamed",6370000,0]],PRIMEM["Greenwich",0],\
                            UNIT["degree",0.0174532925199433]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",64],PARAMETER["central_meridian",-152],\
                            PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1],\
                            EXTENSION["PROJ4","+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +wktext"]]',
        restacked_by='Scenarios Network for Alaska + Arctic Planning -- 2018',
        SNAP_VERSION=snap_version )
    
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

    new_ds[ 'xc' ].attrs = crs_attrs
    new_ds[ 'yc' ].attrs = crs_attrs

    # level attrs (if needed / available)
    if level_attrs is not None:
        new_ds[ levelname ].attrs = level_attrs

    # lon/lat attrs
    lon_attrs = OrderedDict({'long_name':'Longitude','units':'degrees','grid_notes':'irregular grid representing centroids of locations in Cartesian grid of \
                                                                                        Polar Stereographic in the Geographic WGS84 coordinates. These come directly from WRF.'})
    lat_attrs = OrderedDict({'long_name':'Latitude','units':'degrees','grid_notes':'irregular grid representing centroids of locations in Cartesian grid of \
                                                                                        Polar Stereographic in the Geographic WGS84 coordinates. These come directly from WRF.'})
    new_ds[ 'lon' ].attrs = lon_attrs
    new_ds[ 'lat' ].attrs = lat_attrs

    # VARIABLE attributes
    var_attrs = OrderedDict()
    var_attrs.update( long_name=var_attrs_lookup[ variable ][ 'long_name' ],
                      units=var_attrs_lookup[ variable ][ 'units' ],
                      coordinates_="xc yc",
                      temporal_resampling='None: these data represent hourly outputs from wrf dynamical downscaling.' 
                    )
                
    new_ds[ variable.lower() ].attrs = var_attrs

    # write it back out to disk with compression encoding
    encoding = new_ds[ variable.lower() ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    new_ds[ variable.lower() ].encoding = encoding
    
    try:
        dirname = os.path.dirname( out_fn )
        if not os.path.exists( dirname ):
            os.makedirs( dirname )
    except:
        pass

    # close the open file handle and remove the file to rewrite it out...  
    ds.close()
    # os.remove( fn )

    new_ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )
    new_ds.close()

    # using the base netCDF4 package update the times to be UTC and dump back to disk
    # hacky but overcomes a current somewhat limitation in xarray.
    retval = force_update_times_UTC( out_fn )

    return retval


if __name__ == '__main__':
    import numpy as np
    import xarray as xr
    from collections import OrderedDict
    import os
    import multiprocessing as mp
    import argparse

    # parse some args
    parser = argparse.ArgumentParser( description='stack the hourly outputs from raw WRF outputs to NetCDF files of hourlies broken up by year.' )
    parser.add_argument( "-b", "--base_dir", action='store', dest='base_dir', type=str, help="input hourly directory with annual sub-dirs containing hourly WRF NetCDF outputs" )
    parser.add_argument( "-v", "--variable", action='store', dest='variable', type=str, help="variable name to process" )
    parser.add_argument( "-n", "--ncpus", action='store', dest='ncpus', type=int, help="number of cpus to use" )
    
    # parse the args and unpack
    args = parser.parse_args()
    base_dir = args.base_dir
    variables = [ args.variable, args.variable.upper(), args.variable.lower() ] # all combos and one that might be CamelCase
    ncpus = args.ncpus

    # versioning
    snap_version = '0.3'

    # # base directory
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'

    # list the data -- some 4d groups need some special attention...
    files = filelister( base_dir )

    # pull out the variables we want to process on the current node make sure we only have one of each
    files = set([ fn for fn in files for v in variables if ''.join([os.path.sep,v,'_']) in fn ])

    # below is used for building some attrs into the files...
    # raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    raw_fn = '/storage01/pbieniek/gfdl/hist/hourly/1970/WRFDS_d01.1970-02-22_21.nc' # not raw, but pre-processed by Peter
    var_attrs_lookup = make_variable_lookup( raw_fn )

    # Add T2min / T2max as special cases to the var_attrs_lookup dict() since they are derived variables.
    var_attrs_lookup[ 'T2min' ] = {'long_name': 'TEMP at 2 M -- Min(24hr)', 'units': 'K'}
    var_attrs_lookup[ 'T2max' ] = {'long_name': 'TEMP at 2 M -- Max(24hr)', 'units': 'K'}
    var_attrs_lookup[ 'lv_DBLY3' ] = {'long_name': 'layer between two depths below land surface', 'units': 'cm'}

    # run
    pool = mp.Pool( ncpus )
    out = pool.map( run, files )
    pool.close()
    pool.join()
