def make_args( base_path, agg_group='hourly' ):
    wildcard = '*.nc' 
    variables = [ os.path.basename(i).upper() for i in glob.glob( os.path.join(base_path, agg_group, '*' ) ) 
                    if os.path.isdir( i ) and 'slurm' not in i ]
    
    args = []
    for variable in variables:
        files = sorted( glob.glob( os.path.join( base_path, agg_group, variable.lower(), wildcard ) ) )
        varnames = [variable] * len( files )
        args = args + list(zip(varnames, files))
    return args

def resample( fn, variable, agg_str='D' ):
    print( 'working on: {} - {}'.format( fn, variable ) )

    ds = xr.open_dataset( fn, autoclose=True )
    # get some attrs
    global_attrs = ds.attrs
    local_attrs = ds[ variable ].attrs
    xy_attrs = ds.lon.attrs
    time_attrs = ds.time.attrs

    # pathing
    # input_path = os.path.join( base_path, 'hourly', variable )
    output_path = os.path.join( base_path, 'daily', variable.lower() )

    try:
        if not os.path.exists( output_path ):
            os.makedirs( output_path )
    except:
        pass # since it is a parallel op

    # metric switch -- aggregation
    if variable in ['PCPT','pcpt', 'PCPC', 'pcpc' ,'PCPNC', 'pcpnc', 'ACSNOW', 'acsnow']:
        metric = 'sum'
        ds_day = ds.resample( time=agg_str ).sum()
    else:
        metric = 'mean'
        ds_day = ds.resample( time=agg_str ).mean()

    ds_day_comp = ds_day.compute() # watch this one

    # ds_day_comp = ds_day_comp.to_dataset( name=variable )
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
    output_filename = os.path.join( output_path, basename )

    ds_day_comp.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

    # cleanup file handles
    ds.close()
    ds = None
    ds_day.close()
    ds_day = None
    ds_day_comp.close()
    ds_day_comp = None
    return output_filename

def wrap( x ):
    variable, fn = x
    return resample( fn, variable )

if __name__ == '__main__':
    # resample hourly to monthly for data delivery
    import xarray as xr
    import numpy as np
    import pandas as pd
    import os, glob, itertools
    import multiprocessing as mp

    base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables'
    args = make_args( base_path, agg_group='hourly' )
    # args = [ (i,j) for i,j in args if i == 'POTEVP' ] # REMOVE temporary to split it up for speed...
    ncpus = 40

    # parallel process
    pool = mp.Pool( ncpus )
    out = pool.map( wrap, args )
    pool.close()
    pool.join()
