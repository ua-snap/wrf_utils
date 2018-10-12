# fix the leap year timing issue...  
def fix_time_leap( fn ):
    out_fn = fn.replace('.nc', '_fixleap.nc')
    year = fn.split('.nc')[0].split('_')[-1]
    variable = os.path.basename(fn).split('.nc')[0].split('_')[0]
    # fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/tsk/tsk_hourly_wrf_GFDL-CM3_rcp85_{}.nc'.format(year)
    with xr.open_dataset( fn ) as ds:
        dates = pd.date_range('{}-01-01 00'.format(year), '{}-12-31 23'.format(year), freq='H')
        # pop out the leap day
        new_times = pd.DatetimeIndex([ d for d in dates if d.strftime('%m-%d') != '02-29' ])
        out_ds = ds.copy(deep=True)
        out_ds['time'] = new_times
        out_ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )
        out_ds.close()
        out_ds = None
    print( 'completed: {} - {}'.format( year, variable ) )
    return out_fn

if __name__ == '__main__':
    import xarray as xr
    import pandas as pd
    import numpy as np
    import calendar, glob, os
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
    variables = args.variable
    ncpus = args.ncpus

    # # # # # 
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'
    # variable = 'tsk'
    # ncpus = 10
    # # # # # 

    # which are leaps
    years = np.arange(1970,2100+1)
    leap_years = years[[ calendar.isleap(year) for year in years ]]

    # list files
    files = [ fn for fn in glob.glob( os.path.join(base_dir, variable, '*.nc') ) if 'ERA-Interim' not in fn and int(fn.split('.nc')[0].split('_')[-1]) in leap_years and '_fixleap.nc' not in fn ]

    # run it in parallel
    pool = mp.Pool( ncpus )
    out = pool.map( fix_time_leap, files )
    pool.close()
    pool.join()

