def closest_point( lon, lat, da ):
    ''' the spatial way to do the lookup aka the RIGHT WAY  :)'''
    from shapely.geometry import Point
    from shapely.ops import nearest_points, unary_union

    lons = np.array(da.lon).ravel()
    lats = np.array(da.lat).ravel()

    pts = unary_union([ Point(*i) for i in zip(lons, lats)])
    pt = Point(lon, lat)

    # get the nearest point
    nearest = nearest_points(pt, pts)[1]
    return nearest.x, nearest.y

def get_profile( fn, x, y, variable ):
    ''' return profile of values from NetCDF4 '''
    
    # open dset
    ds = xr.open_dataset( fn )
    da = ds[ variable ] # get the array as it is easier to process on

    # extract
    lonc, latc = closest_point( lon, lat, da )
    da_pt = da.where( (da.lat == latc) & (da.lon == lonc), drop=True )

    # resample to daily
    da_pt_daily = da_pt.resample( time='D' ).mean()
    ds.close()
    ds = None; del ds

    return np.array( da_pt_daily )

def wrap( x ):
    ''' simple multiprocessing wrapper '''
    return get_profile( *x )

def run( files, lon, lat, variable, ncpus=32 ):
    ''' run the extraction across all given data groups '''
    
    # multiprocessing args
    args = [ (fn,lon,lat,variable) for fn in files ]

    # run in parallel
    pool = mp.Pool( ncpus )
    out = pool.map( wrap, args )
    pool.close()
    pool.join()

    # concat arrays
    return np.concatenate( out )
    

if __name__ == '__main__':
    import os, glob
    import numpy as np
    import pandas as pd
    import xarray as xr
    import multiprocessing as mp
    
    # setup some global args
    variable = 'T2'
    lat,lon = (63.9719, -145.706)
    group = 'hourly'
    out_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/extracted_profiles_dod'

    # set the path we want to get the data from 
    path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/{}/{}'.format(group,variable.lower())

    # models
    models = ['gfdl_rcp85','erain','gfdl_hist']
    time_lookup_daily = { 'gfdl_rcp85':('2006-01-02','2100-12-31'),'erain':('1979-01-02','2015-10-29'),'gfdl_hist':('1970-01-02','2005-12-31') }

    for model in models:
        print( 'running:{}'.format( model ) )
        
        # list files
        files = sorted( glob.glob( os.path.join( path, '*{}*.nc'.format(model) ) ) )
            
        # fix files here...
        if model == 'gfdl_hist' and '2006' in files[-1]:
            files = files[:-1]

        # build time index
        begin, end = time_lookup_daily[ model ]
        time = pd.date_range( begin, end, freq='D' )

        if model != 'erain':
            # drop leap days
            time = [t for t in time if not (t.day == 29 and t.month == 2) ]
            
        # run 
        data_out = run(files, lon, lat, variable)

        # conversion
        if variable == 'T2':
            data_out = np.around(data_out - 273.15, 1)

        # dump to csv
        df = pd.DataFrame( { model : data_out }, index=time )

        out_fn = os.path.join( out_path, '{}_mean_day_{}_wrf_FortGreely_AK.csv'.format(variable,model) )
        dirname = os.path.dirname( out_fn )
        if not os.path.exists( dirname ):
            os.makedirs( dirname )

        df.to_csv( out_fn, sep=',' )

        del files, time, data_out, df
