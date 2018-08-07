# resample hourly T2 to daily T2 min/max

def make_args( base_path, agg_group='hourly' ):
    wildcard = '*.nc' 
    variable = 'T2'

    files = sorted( glob.glob( os.path.join( base_path, agg_group, variable.lower(), wildcard ) ) )
    varnames = [variable] * len( files )
    return list( zip( varnames, files ) )

def resample( fn, variable, agg_str='D' ):
    from collections import OrderedDict
    print( 'working on: {} - {}'.format( fn, variable ) )

    ds = xr.open_dataset( fn, autoclose=True )
    # get some attrs
    global_attrs = ds.attrs
    local_attrs = ds[ variable ].attrs
    xy_attrs = ds.lon.attrs
    time_attrs = ds.time.attrs

    for metric in ['min', 'max']:
        if metric == 'min':
            # new_varname = 'T2min'
            ds_day = ds.resample( time=agg_str ).min()

        elif metric == 'max':
            # new_varname = 'T2max'
            ds_day = ds.resample( time=agg_str ).max()
        else:
            BaseException( 'this tool is only for Min/Max aggregations from hourly to daily' )
        
        ds_day_comp = ds_day.compute() # watch this one
        ds_day_comp.attrs = global_attrs
        ds_day_comp[ variable ].attrs = local_attrs
        ds_day_comp[ 'lon' ].attrs = xy_attrs
        ds_day_comp[ 'lat' ].attrs = xy_attrs
        ds_day_comp[ 'time' ].attrs = time_attrs

        # set output compression and encoding for serialization
        encoding = ds_day_comp[ variable ].encoding
        encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
        ds_day_comp[ variable ].encoding = encoding
        
        # dump to disk
        dirname, basename = os.path.split( fn )
        # [ WATCH ] this is hardwired to hourly / daily...
        basename = basename.replace( 'hourly', 'daily' ).replace( '.nc', '_{}.nc'.format(metric) )

        # modify the variable name
        if metric == 'min':
            ds_day_comp = ds_day_comp.rename({'T2':'T2min'})
            ds_day_comp['T2min'].attrs = OrderedDict({'long_name':'Daily Temperature Minimum Derived from Hourlies', 'description':'hourly data were resampled / aggregated to daily using the hourly minimum data for the given day'})
            basename = basename.replace('T2', 'T2min')
            output_path = os.path.join( base_path, 'daily', 't2min' )
            output_filename = os.path.join( output_path, basename )
        elif metric == 'max':
            ds_day_comp = ds_day_comp.rename({'T2':'T2max'})
            ds_day_comp['T2max'].attrs = OrderedDict({'long_name':'Daily Temperature Maximum Derived from Hourlies', 'description':'hourly data were resampled / aggregated to daily using the hourly maximum data for the given day'})
            basename = basename.replace('T2', 'T2max')
            output_path = os.path.join( base_path, 'daily', 't2max' )
            output_filename = os.path.join( output_path, basename )
    
        try:
            if not os.path.exists( output_path ):
                os.makedirs( output_path )
        except:
            pass # since it is a parallel op

        ds_day_comp.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

        # cleanup file handles
        ds_day.close()
        ds_day = None
        ds_day_comp.close()
        ds_day_comp = None

    # cleanup
    ds.close()
    ds = None

    return output_filename

def wrap( x ):
    variable, fn = x
    return resample( fn, variable )

if __name__ == '__main__':
    import xarray as xr
    import numpy as np
    import pandas as pd
    import os, glob, itertools
    import multiprocessing as mp

    base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data'
    args = make_args( base_path, agg_group='hourly' )
    ncpus = 32

    # parallel process
    pool = mp.Pool( ncpus )
    out = pool.map( wrap, args )
    pool.close()
    pool.join()
