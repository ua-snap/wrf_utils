# IMPROVE THE SNAP OUTPUTS FOR DISTRIBUTION
# THIS IS A LIVING AND CHANGING SCRIPT AS I LEARN MORE ABOUT THE FILE STRUCTURE
# AND CF-CONVENTIONS AND FROM OTHERS NEEDS, I AM UPDATING THE FILE METADATA AND 
# COORDS / DIMS APPROPRIATELY. 
# 
# March 2018 - Michael Lindgren
# # # # # # # # # # # # # # # # # # # # # 

# # # # TO COMPLETE:
# 1. add in attrs for lon / lat coords
# 2. make sure xc / yc are the names we want and if not, change it.
# 3. set attrs for xc / yc and review coords attrs across the dataset.
# 4. run a test set of files and run some additional tests on the outputs to ensure things
#     are working as expected.


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

    data = ds[ variable ].values
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
    dat = {i:{'long_name':raw[i].description, 'units':raw[i].units} for i in raw.variables.mapping.keys() if hasattr(raw[i], 'description') }
    # update missing variables from the list as we have found them....
    dat.update( PCPT={'long_name': 'Total precipitation', 'units':'mm'},
                QBOT={'long_name': 'Specific humidity at lowest model level', 'units':'kg/kg'} )
    return dat

def filelister( base_dir ):
    return [ os.path.join(r,fn) for r,s,files in os.walk( base_dir ) 
                                for fn in files if fn.endswith('.nc') ]

if __name__ == '__main__':
    import numpy as np
    import xarray as xr
    from collections import OrderedDict
    import os

    # versioning
    snap_version = '0.1b'

    # base directory
    base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/daily'

    # list the data 
    files = filelister( base_dir )

    # below is used for building some attrs into the files...
    raw_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_raw_output_example/wrfout_d01_2025-07-10_00:00:00'
    var_attrs_lookup = make_variable_lookup( raw_fn )
    
    # done = ['/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/pcpt/pcpt_wrf_GFDL-CM3_historical_monthly_1970-2005_sum.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/pcpt/pcpt_wrf_GFDL-CM3_rcp85_monthly_2006-2100_sum.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/pcpt/pcpt_wrf_ERA_Interim_monthly_1979-2015_sum.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snowc/snowc_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snowc/snowc_wrf_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snowc/snowc_wrf_ERA_Interim_monthly_1979-2015_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/vegfra/vegfra_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/vegfra/vegfra_wrf_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/vegfra/vegfra_wrf_ERA_Interim_monthly_1979-2015_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qbot/qbot_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qbot/qbot_wrf_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qbot/qbot_wrf_ERA_Interim_monthly_1979-2015_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/t2/t2_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/t2/t2_wrf_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/t2/t2_wrf_ERA_Interim_monthly_1979-2015_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snow/snow_wrf_GFDL-CM3_historical_monthly_1970-2005_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snow/snow_wrf_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc','/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/snow/snow_wrf_ERA_Interim_monthly_1979-2015_mean.nc']
    # files = ['/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/tslb/tslb_wrf_compress_GFDL-CM3_historical_monthly_1970-2005_mean.nc',
    #      '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/tslb/tslb_wrf_compress_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc',
    #      '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/tslb/tslb_wrf_compress_ERA_Interim_monthly_1979-2015_mean.nc',
    #      '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qvapor/qvapor_wrf_compress_GFDL-CM3_historical_monthly_1970-2005_mean.nc',
    #      '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qvapor/qvapor_wrf_compress_GFDL-CM3_rcp85_monthly_2006-2100_mean.nc',
    #      '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly/qvapor/qvapor_wrf_compress_ERA_Interim_monthly_1979-2015_mean.nc']
    
    # loop through the files
    for fn in files:
        print( fn )

        # make an output name from the input name.
        out_fn = fn.replace('wrf_data/daily','wrf_data/daily_fix')

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

        if len(flipped['data'].shape) == 3:
            # build a new NetCDF and dump to disk -- with compression
            new_ds = xr.Dataset({variable: (['time','xc', 'yc'], flipped['data'])},
                                coords={'xc': ('xc', np.flipud(x)[0,]),
                                        'yc': ('yc', np.flipud(y)[0,]),
                                        'lon':(['xc','yc'], np.flipud(ds.lon.data) ),
                                        'lat':(['xc','yc'], np.flipud(ds.lat.data) ),
                                        'time':time })
            
        elif len(flipped['data'].shape) == 4: #(time,levels, x, y )
            # levelname to use if 4D
            if variable in ['TSLB', 'SMOIS']:
                levelname = 'lv_DBLY3'
            else:
                levelname = 'lv_ISBL2'

            # build dataset with levels at each timestep
            new_ds = xr.Dataset( {variable:(['time',levelname,'xc', 'yc'], flipped['data'])},
                        coords={'xc': ('xc', np.flipud(x)[0,]),
                                'yc': ('yc', np.flipud(y)[0,]),
                                'lon':(['xc','yc'], np.flipud(ds.lon.data) ),
                                'lat':(['xc','yc'], np.flipud(ds.lat.data) ),
                                'time': time,
                                levelname:ds[ levelname ]})
        else:
            raise BaseException( 'wrong number of dimensions' )

        # update some file attrs...
        proj4string = '+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000'
        
        # native file Attributes
        base_attrs.update( proj_parameters=proj4string, 
                restacked_by='Scenarios Network for Alaska + Arctic Planning -- November/December 2017',
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

        # VARIABLE attributes
        var_attrs = OrderedDict()
        var_attrs.update( long_name=var_attrs_lookup[ variable ][ 'long_name' ],
                           units=var_attrs_lookup[ variable ][ 'units' ],
                           # coordinates_="xc yc"
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

        # close the open file handle and remove the file to rewrite it out...  
        ds.close()
        ds = None
        del ds
        os.remove( fn )

        new_ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )