import xarray as xr

# t2 directory contains all required year inputs
file_prefix = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_hourly_wrf_GFDL-CM3_rcp85_'

files = []
for i in range (2010,2100,10):
        for j in range(0,10):
                year = i + j
                files.append(file_prefix + str(year) + '.nc')
        ds = xr.open_mfdataset(files)
        ds_mean = ds.mean('time') - 273.15
        # create decadal directory first
        filename = 'decadal/decadal_mean_' + str(i) + '-' + str(i + 9) + '.nc'
        print filename
        ds_mean.to_netcdf(filename, mode='w', format='NETCDF4_CLASSIC')
        files = []


# one way to do it that employs dask. which is useful for machines with minimal RAM / cores
if __name__ == '__main__':
    import glob, os
    import xarray as xr
    import dask

    # base_path = '/whatever/the/dir/path/is'
    base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'
    variable = 't2'
    group = 'rcp85'

    # list the files and sort em
    files = sorted( glob.glob( os.path.join(base_path,variable,'*{}*.nc'.format(group)) ) )

    # since we know that the filenames end in the '_year.nc' we can slice that out
    decades = [ int(fn.split('.')[0].split('_')[-1][:3]+'0') for fn in files ]

    # open the files as an mfdataset which will employ dask.
    # this is sometimes not the fastest way to do it, but it works and is clean
    ds = xr.open_mfdataset( files )
    dec_avg = ds.groupby( decades ).mean( 'time' ).compute()

    # dump the new file to disk
    out_fn = '/path/to/some_output_filename.nc'
    dec_avg.to_netcdf( out_fn )

# base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'

# a way to do it to utilize all cores on an atlas node.
def make_decadal_avg( files ):
    ds = xr.open_mfdataset( files )
    return ds.mean( 'time' ).compute()

if __name__ == '__main__':
    import glob, os
    import pandas as pd
    import xarray as xr
    import numpy as np
    import dask
    import multiprocessing as mp

    base_path = './wrf_data/hourly_fix'
    variable = 't2'
    group = 'rcp85'
    ncpus = 32
    out_fn = '/{}_decadal_mean.nc'.format(variable)
    
    # list the files and sort em --> this can be made more dynamic, but follows Python conventions
    files = sorted( glob.glob( os.path.join(base_path,variable,'*{}*.nc'.format(group)) ) )

    # since we know that the filenames end in '_{year}.nc' we can slice that out
    # and do some string-fu to make it a decade --> ...this can be made more dynamic...
    decades = [ int(fn.split('.')[0].split('_')[-1][:3]+'0') for fn in files ]

    # group the files using the pandas mechanics.
    grouped = [j.tolist() for i,j in pd.Series(files).groupby(decades) if len(j) == 10]

    # run the decades in parallel
    pool = mp.Pool( ncpus )
    out = pool.map( make_decadal_avg, grouped )
    pool.close()
    pool.join()

    # pull just the 2D array from the new files.
    arr = np.array([ i[variable].values for i in out ])

    # make the decades to pass to the NetCDF --> this can be made more dynamic, but works for rcp85
    dates = np.arange(2010,2090+1,10)

    # pull some coords from one of the files
    with xr.open_dataset(files[0]) as tmp_ds:
        xc = tmp_ds.xc.values
        yc = tmp_ds.yc.values
        # more could be pulled from the file here if we wanted/cared about more metadata from the orig files

    # make a new NetCDF file 
    new_ds = xr.Dataset({variable: (['decade','yc', 'xc'], arr)},
                    coords={'xc': ('xc', xc),
                            'yc': ('yc', yc),
                            'decade':dates })

    new_ds.to_netcdf( out_fn )



