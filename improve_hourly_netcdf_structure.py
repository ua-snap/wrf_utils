# IMPROVE THE SNAP OUTPUTS FOR DISTRIBUTION
# THIS IS A LIVING AND CHANGING SCRIPT AS I LEARN MORE ABOUT THE FILE STRUCTURE
# AND CF-CONVENTIONS AND FROM OTHERS NEEDS, I AM UPDATING THE FILE METADATA AND 
# COORDS / DIMS APPROPRIATELY. 
# 
# June 2018 - Michael Lindgren (malindgren@alaska.edu)
# # # # # # # # # # # # # # # # # # # # # 

def get_meta_from_wrf( ds ):
    """Get the WRF projection/coords and other metadata out of the file."""
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
    x =  ds[ 'lon' ].values
    y = np.flipud( ds[ 'lat' ].values )

    if len(data.shape) == 3:
        dat = np.rollaxis( np.dstack([ np.flipud( ts ) for ts in data ]), -1)
        out = { 'data':dat, 'x':x, 'y':y }
    elif len(data.shape) == 4:
        dat = np.stack([ np.flip(ts, axis=1) for ts in data ])
        out = { 'data':dat, 'x':x, 'y':y }
    return out

def make_variable_lookup( raw_fn ):
    '''make a lookup table of variables and some metadata from the RAW WRF files.'''
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

def run( fn, meta ):
    ''' 
    take a wrf file that has been re-stacked by SNAP, but needs better metadata
    and build a new output file with that new metadata.
    
    fn = [str] the path to the wrf output file (that has been modified by the WRF group here at IARC)
    meta = [dict] a meta dict of file attributes and coordinates built using the `get_meta_from_wrf` function.

    '''
    print( 'running: {}'.format( fn ) )

    # make an output name from the input name.
    out_fn = fn.replace( '/hourly','/hourly_fix' )
    dirname, basename = os.path.split( out_fn )
    basename, ext = os.path.splitext( basename )

    # [NOTE]: naming below is hardwired to a naming that is identical (element-wise) 
    #    to this: 'ACSNOW_wrf_hourly_gfdl_rcp85_2018' (.nc removed above)

    name_elems = basename.split( '_' )
    if not len(name_elems) > 6:
        variable, group, timestep, model, scenario, year = name_elems
    else:
        variableA, variableB, group, timestep, model, scenario, year = name_elems
        variable = '_'.join([variableA, variableB])

    modelnames = {'gfdl':'GFDL-CM3','era':'ERA-Interim'}
    scenarionames = {'rcp85':'rcp85', 'hist':'historical','interim':'historical'}
    # metric_lookup = {'min':'Minimum', 'max':'Maximum', 'sum':'Sum/Total', 'mean':'Mean/Avg'}

    basename = '_'.join([variable.lower(),timestep,group,modelnames[model],scenarionames[scenario],year]) + ext
    out_fn = os.path.join( dirname, basename )

    # open the dataset to restructure / improve
    ds = xr.open_dataset( fn, autoclose=True )
    
    # get the variable name from the already stacked files (produced by SNAP)
    variable = get_variable_name( ds )
    flipped = flip_data_and_coords( ds, variable=variable )
    
    # make some file coords and attrs from the file metadata we built
    x,y = meta['xc'], meta['yc']
    time = ds['time']
    base_attrs = ds.attrs.copy()
    
    # update time attributes.
    time_attrs = time.attrs.copy()
    time_attrs.update( {'time zone': 'all times are UTC.'})
    time.attrs = time_attrs

    # pull out the lat / lons from the input file to toss into the cleaned stacked outputs
    lons = ds.lon.data
    lats = np.flipud( ds.lat.data ) # so they are north-up! which makes it easier and pythonic.

    if len(flipped['data'].shape) == 3:
        # build a new NetCDF and dump to disk -- with compression
        new_ds = xr.Dataset({variable.lower(): (['time','yc', 'xc'], flipped['data'])},
                            coords={'xc': ('xc', x[0,]),
                                    'yc': ('yc', y[:,0]),
                                    'lon':(['yc','xc'], lons ),
                                    'lat':(['yc','xc'], lats ),
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
                    coords={'xc': ('xc', x[0,]),
                            'yc': ('yc', y[:,0]),
                            'lon':(['yc','xc'], lons ),
                            'lat':(['yc','xc'], lats ),
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
                    ('coordinate_location', 'centroid'),
                    ('origin_upperleft_x_corner', meta['origin'][0]),
                    ('origin_upperleft_y_corner', meta['origin'][1])
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
    ds = None

    new_ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )
    new_ds.close()
    new_ds = None

    # using the base netCDF4 package update the times to be UTC and dump back to disk
    # hacky but overcomes a current somewhat limitation in xarray.
    out_fn = force_update_times_UTC( out_fn )

    return out_fn

if __name__ == '__main__':
    import numpy as np
    import xarray as xr
    from collections import OrderedDict
    import os
    from functools import partial
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
    snap_version = '0.4'

    # # # # BEGIN TEST
    # # # base directory
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'
    # variable = 'PCPT'
    # ncpus = 10
    # variables = [ variable, variable.upper(), variable.lower() ] # all combos and one that might be CamelCase
    # # # # END TEST

    # list the data -- some 4d groups need some special attention...
    files = filelister( base_dir )

    # pull out the variables we want to process on the current node make sure we only have one of each
    files = set([ fn for fn in files for v in variables if ''.join([os.path.sep,v,'_']) in fn ])

    # this file is the RAW output from WRF before Peter performs some cleanup of the data.  This is VERY IMPORTANT for proper file metadata
    wrf_raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    with xr.open_dataset( wrf_raw_fn ) as ds:
        meta = get_meta_from_wrf( ds )

    # below is used for building some attrs into the files...
    raw_fn = '/storage01/pbieniek/gfdl/hist/hourly/1970/WRFDS_d01.1970-02-22_21.nc' # not raw, but pre-processed by Peter
    var_attrs_lookup = make_variable_lookup( raw_fn )

    # Add T2min / T2max as special cases to the var_attrs_lookup dict() since they are derived variables.
    var_attrs_lookup[ 'T2min' ] = {'long_name': 'TEMP at 2 M -- Min(24hr)', 'units': 'K'}
    var_attrs_lookup[ 'T2max' ] = {'long_name': 'TEMP at 2 M -- Max(24hr)', 'units': 'K'}
    var_attrs_lookup[ 'lv_DBLY3' ] = {'long_name': 'layer between two depths below land surface', 'units': 'cm'}

    # run
    f = partial( run, meta=meta )
    pool = mp.Pool( ncpus )
    out = pool.map( f, files )
    pool.close()
    pool.join()
