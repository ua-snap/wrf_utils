def make_args( base_path, agg_group='daily' ):
    # wildcard = '*.nc' 
    variables = [ os.path.basename(i).upper() for i in glob.glob( os.path.join(base_path, agg_group, '*' ) ) 
                    if os.path.isdir( i ) and 'slurm' not in i ]

    groups = ['ERA-Interim', 'GFDL-CM3_historical', 'GFDL-CM3_rcp85']
    years_lu = {'ERA-Interim':('1979','2015'), 'GFDL-CM3_historical':('1970','2006'), 'GFDL-CM3_rcp85':('2006','2100')}
    
    args = [ (variable,sorted( glob.glob( os.path.join( base_path, agg_group, variable.lower(), '*{}*.nc'.format(wildcard) ) ) ), years_lu[wildcard]) 
                    for variable in variables
                        for wildcard in groups ]
    return args

def resample( files, variable, begin_year, end_year, agg_str='M' ):
    print( 'working on: {}'.format( variable ) )

    ds = xr.open_mfdataset( files, autoclose=True )
    
    # get some attrs
    global_attrs = ds.attrs
    variable_attrs = ds[ variable ].attrs

    output_path = os.path.join( base_path, 'monthly_fix', variable.lower() )

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

    # update attrs
    metric_lookup = {'sum':'Sum/Total', 'mean':'Mean/Avg'}
    variable_attrs.update( temporal_resampling='Monthlies generated from dailies using {}'.format( metric_lookup[metric] ) )
    
    ds_day_comp.attrs = global_attrs
    ds_day_comp[ variable ].attrs = variable_attrs
    ds.time.attrs.update( timestep='monthly {} aggregated from daily'.format( metric ) )

    # set output compression and encoding for serialization
    encoding = ds_day_comp[ variable ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    ds_day_comp[ variable ].encoding = encoding
    
    # dump to disk
    fn = files[0]
    dirname, basename = os.path.split( fn )
    # [ WATCH ] this is hardwired to daily / monthly...
    basename = basename.replace( 'daily', 'monthly' )
    # get single filename elements for output_filename
    elems = basename.split('.')[0].split('_')
    _ = elems.pop(-1) # remove the single year...
    elems.append( '{}-{}'.format(begin_year, end_year) ) # put in full year range to filename

    output_filename = os.path.join( output_path, '_'.join(elems)+'.nc' )
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
    variable, files, years = x
    # handle nonstandard SNAP-derived variables
    if variable == 'T2MIN':
        variable = 'T2min'
    elif variable == 'T2MAX':
        variable = 'T2max'
    begin_year, end_year = years
    return resample( files, variable, begin_year, end_year )

if __name__ == '__main__':
    # resample hourly to monthly for data delivery
    import xarray as xr
    import numpy as np
    import pandas as pd
    import os, glob, itertools
    import multiprocessing as mp

    base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data'
    args = make_args( base_path, agg_group='daily_fix' )
    # args = [ (i,j,k) for i,j,k in args if i == 'POTEVP' ]
    ncpus = 25

    # parallel process
    pool = mp.Pool( ncpus )
    out = pool.map( wrap, args )
    pool.close()
    pool.join()
