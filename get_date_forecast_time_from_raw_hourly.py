# get forecast_time attrs from dset to use in interpolation
def open_ds( fn, variable ):
    ''' cleanly read variable/close a single hourly netcdf '''
    import xarray as xr
    ds = xr.open_dataset( fn, autoclose=True )
    out = ds[ variable ].copy()
    ds.close()
    return out
def list_files( dirname ):
    '''
    list the files and split the filenames into their descriptor parts and 
    return dataframe of elements and filename sorted by:['year', 'month', 'day', 'hour']
    '''
    import os
    import pandas as pd

    files = [ get_month_day( os.path.join(r, fn)) for r,s,files in os.walk( dirname ) 
                        for fn in files if os.path.split(r)[-1].isdigit() and fn.endswith( '.nc' )
                         and '-*.nc' not in fn and 'old' not in r and 'test' not in r ]

    files_df = pd.DataFrame( files )
    return files_df.sort_values( ['year', 'month', 'day', 'hour'] ).reset_index()
def get_month_day( fn ):
    dirname, basename = os.path.split( fn )
    year, month, day_hour = basename.split('.')[-2].split('-')
    day, hour = day_hour.split( '_' )
    folder_year = dirname.split('/')[-1]
    return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}
def get_forecast_time( fn ):
    return open_ds( fn, variable='PCPT' ).attrs['forecast_time']
def get_file_attrs( fn ):
    try:
        fn_args = get_month_day( fn )
        fn_args.update( forecast_time=get_forecast_time( fn ) )
    except:
        # if there is an issue... dont fail, do this...
        nodata = -9999
        fn_args = {'fn':fn, 'year':nodata, 'folder_year':nodata,'month':nodata, 'day':nodata, 'hour':nodata, 'forecast_time':nodata}
    return fn_args

if __name__ == '__main__':
    import xarray as xr
    import pandas as pd
    import multiprocessing as mp
    import os
    
    # setup args
    base_path = '/storage01/pbieniek/gfdl/hist/hourly'
    # base_path = '/storage01/pbieniek/erain/hourly'
    # base_path = '/storage01/rtladerjr/hourly'
  
    # fn_list = [ os.path.join( r, fn ) for r,s,files in os.walk( base_path ) 
    #           if 'oldstuff' not in r for fn in files if fn.endswith( '.nc' ) 
    #               and 'test' not in fn and '-*.nc' not in fn ]
    
    fn_list = list_files( base_path )
    # drop unneeded duplicates
    fn_list = fn_list[ fn_list.folder_year == fn_list.year ]
    print( 'number of files: {}'.format(len( fn_list )) )

    ncpus = 32
    output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs'
    group = 'gfdl_hist'
    # group = 'erain'
    # group = 'gfdl_rcp85'

    pool = mp.Pool( ncpus )
    print( 'start multiprocessing' )
    out = [ pool.map( get_file_attrs, fnl['fn'] ) for group, fnl in fn_list.groupby( 'year' ) ]
    out = [ j for i in out for j in i ]
    print( 'multiprocessing complete' )
    pool.close()
    pool.join()

    print( 'df' )
    df = pd.DataFrame( out )
    output_filename = os.path.join( output_path, 'WRFDS_forecast_time_attr_{}.csv'.format( group ) )
    print( 'writing to disk' )
    df.to_csv( output_filename, sep=',' )
