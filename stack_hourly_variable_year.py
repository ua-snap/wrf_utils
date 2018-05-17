#! ~/v3/bin/python3

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
# stack the hourly data and diff/interpolate the accumulation variables
#   final version: August 8, 2017 malindgren@alaska.edu
#   LICENSE: MIT
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def nan_helper( y ):
    '''
    Helper to handle indices and logical indices of NaNs.

    Input:
        - y, 1d numpy array with possible NaNs
    Output:
        - nans, logical indices of NaNs
        - index, a function, with signature indices= index(logical_indices),
          to convert logical indices of NaNs to 'equivalent' indices
    Example:
        >>> # linear interpolation of NaNs
        >>> nans, x= nan_helper(y)
        >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])

        https://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array
    '''
    return np.isnan( y ), lambda z: z.nonzero()[0]

def interp_1d_along_axis( y ):
    ''' interpolate across 1D timeslices of a 3D array. '''
    nans, x = nan_helper( y )
    y[nans] = np.interp( x(nans), x(~nans), y[~nans] )
    return y

def open_ds( fn, variable ):
    ''' cleanly read variable/close a single hourly netcdf '''
    import xarray as xr
    with xr.open_dataset( fn ) as ds:
        out = np.array( ds[ variable ].load() ).copy()
    return out

def rolling_window( a, window ):
    ''' simple 1-D rolling window function '''
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

def adjacent_files( df ):
    ''' to be performed on the full DataFrame of pre-sorted files. '''
    # WE LOSE THE FIRST ONE IN THE SERIES...
    adj = rolling_window( df.fn, window=3 ).tolist()
    last = df.iloc[-2:,].fn.tolist()
    first = df.iloc[0, np.where( df.columns == 'fn')[0]].tolist()
    # append to end of the list.
    adj = [first] + adj + [last]
    return adj

def get_first_row_grouper( df ):
    ''' 
    after a diff (T - (T-1)) we lose the first hour. in this i grab
    all the first layer in all the series, its adjacent files and will use 
    these with the other interpolation values of forecast time. for 
    accumulation variables (precip) only 
    '''
    years = df[ df['year'] > df['year'].min() ]['year'].unique()
    years = np.sort( years[years != min(years)] )
    ind = [ df[df['year'] == year ].index[0] for year in years ]
    interp_list = [ df.iloc[[i-1,i,i+1]].fn.tolist() if i-1 > 0 
                else df.iloc[[i,i+1]].fn.tolist() for i in ind ]
    return ind, interp_list

def _run_group( group, variable ):
    '''
    open and diff within a single forecast_time group through forecast_time=6
    
    this is specific processing for only the variables requiring an accumulation fix
    
    '''
    arr = np.array([open_ds(i, variable) for i in group.fn])
    time, height, width = arr.shape
    diff_arr = np.concatenate([ np.broadcast_to( np.array([np.nan]), (1,height,width)), np.diff( arr, axis=0 ) ])
    return diff_arr

def stack_year( df, year, variable, ncores=32 ):
    ''' open and stack a single level dataset '''
    import multiprocessing as mp
    from functools import partial
    
    pool = mp.Pool( ncores )
    f = partial( open_ds, variable=variable )
    files = df[df.year == year].fn.tolist()
    out = np.array( pool.map( f, files ) )
    pool.close()
    pool.join()
    return out

def stack_year_accum( df, year, variable, ncores=15 ):
    ''' stack, diff, interp accumulation variables -- wrf '''
    import multiprocessing as mp
    from functools import partial

    # group the data into forecast_time begin/end groups
    groups = df.groupby((df.forecast_time == 6).cumsum())
    # unpack it.  this is ugly, but I really hate pandas apply.
    groups = [ group for idx,group in groups ]
    # which indexes correspond to the year in question?
    ind = [ count for count, group in enumerate( groups ) if year in group.year.tolist() ]
    
    # test to be sure they are all chronological -- Should be since dataframe is pre-sorted
    assert np.diff( ind ).all() == 1

    # handle beginning and ending years in the series 
    # get the forecast_time groups that overlap with our current year
    #   and the adjacent groups for seamless time-series.
    if year > df.year.min() and year < df.year.max():
        first_outer_group_idx, last_outer_group_idx = (ind[0] - 1, ind[-1] + 1)
        ind = [ first_outer_group_idx ] + ind + [ last_outer_group_idx ]

    elif year == df.year.min():
        last_outer_group_idx = ind[-1] + 1
        ind = ind + [ last_outer_group_idx ]

    elif year == df.year.max():
        first_outer_group_idx = ind[0] - 1
        ind = [ first_outer_group_idx ] + ind

    else:
        AttributeError( 'broken groups and indexing issues...' )

    # get the corresponding groups for current year processing
    groups = [ groups[idx] for idx in ind ]

    # we need some indexing for slicing the output array to ONLY this current year
    groups_df = pd.concat( groups ) # should be chronological
    current_year_ind, = np.where( groups_df.year == year ) # along time dimension
           
    # process groups and concatenate 3D cubes along time axis chronologically
    f = partial( _run_group, variable=variable )
    pool = mp.Pool( ncores )
    arr = np.concatenate( pool.map( f, groups ), axis=0 )
    pool.close()
    pool.join()

    # interpolate across the np.nan's brought in with differencing each forecast_time group
    arr = np.apply_along_axis( interp_1d_along_axis, axis=0, arr=arr )

    # slice back to the current year
    arr = arr[ current_year_ind, ... ]

    # make sure we have no leftover negative precip
    arr[ arr < 0 ] = 0
    return arr

def run_year( df, year, variable ):
    ''' handle accumulation and normal variables and run for a given year '''
    
    ACCUM_VARS = [ 'ACSNOW', 'PCPT', 'PCPC', 'PCPNC', 'POTEVP' ]

    # interpolate accumulation vars at `ind`
    if variable in ACCUM_VARS:
        arr = stack_year_accum( df, year, variable )
    else:
        arr = stack_year( df, year, variable )
    return arr

if __name__ == '__main__':
    import os
    import xarray as xr
    import numpy as np
    import pandas as pd
    from functools import partial
    import pickle
    import argparse

    # parse some args
    parser = argparse.ArgumentParser( description='stack the hourly outputs from raw WRF outputs to NetCDF files of hourlies broken up by year.' )
    parser.add_argument( "-i", "--input_path", action='store', dest='input_path', type=str, help="input hourly directory with annual sub-dirs containing raw WRF NetCDF outputs" )
    parser.add_argument( "-y", "--year", action='store', dest='year', type=int, help="year to process" )
    parser.add_argument( "-f", "--files_df_fn", action='store', dest='files_df_fn', type=str, help="path to the .csv file containing parsed filename and forecast_time already precomputed " )
    parser.add_argument( "-v", "--variable", action='store', dest='variable', type=str, help="variable name (exact)" )
    parser.add_argument( "-o", "--output_filename", action='store', dest='output_filename', type=str, help="output filename for the new NetCDF hourly data for the input year" )
    parser.add_argument( "-t", "--template_fn", action='store', dest='template_fn', type=str, help="monthly template file that is used for passing global metadata to output NC files." )
    
    # parse the args and unpack
    args = parser.parse_args()
    input_path = args.input_path
    year = args.year
    files_df_fn = args.files_df_fn
    variable = args.variable
    output_filename = args.output_filename
    template_fn = args.template_fn

    # # # # # FOR TESTING
    # input_path = '/storage01/rtladerjr/hourly'
    # # input_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/raw_testing_data/2007'
    # group = 'gfdl_rcp85'
    # variable = 'PCPT' # 'PCPT' #
    # files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    # output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/TESTING_SLURM_WRF'
    # # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable )
    # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' )
    # output_filename = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/T2_wrf_hourly_gfdl_rcp85_1990_FULLTEST_FINAL.nc'
    # year = 2007
    # # # # # END TESTING

    # wrf output standard vars -- [hardwired] for now
    lon_variable = 'g5_lon_1'
    lat_variable = 'g5_lat_0'

    # read in pre-built dataframe with forecast_time as a field
    df = pd.read_csv( files_df_fn, sep=',', index_col=0 ).copy()
    # df[ 'interp_files' ] = adjacent_files( df )

    # run stacking of variable through time and deal with accumulations (if needed).
    arr = run_year( df, year, variable )

    # subset the data frame to the desired year -- for naming stuff
    sub_df = df[ (df.year == year) & (df.folder_year == year) ].reset_index()

    # build the output NetCDF Dataset
    new_dates = pd.date_range( '-'.join(sub_df.loc[sub_df.index[0],['month','day','year']].astype(str)), periods=arr.shape[0], freq='1H' )

    # get some template data to get some vars from... -- HARDWIRED...
    mon_tmp_ds = xr.open_dataset( template_fn, decode_times=False )
    tmp_ds = xr.open_dataset( sub_df.fn.tolist()[0] )
    global_attrs = tmp_ds.attrs.copy()
    global_attrs[ 'reference_time' ] = str(new_dates[0]) # 1979 hourly does NOT start at day 01...  rather day 02....
    global_attrs[ 'proj_parameters' ] = "+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs"
    local_attrs = tmp_ds[ variable ].attrs.copy()
    xy_attrs = mon_tmp_ds.lon.attrs.copy()

    if len( arr.shape ) == 3:
        # build a new Dataset with the stacked timesteps and some we extracted from the input Dataset
        ds = xr.Dataset( {variable:(['time','x', 'y'], arr.astype(np.float32))},
                        coords={'lon': (['x', 'y'], tmp_ds[lon_variable].data),
                                'lat': (['x', 'y'], tmp_ds[lat_variable].data),
                                'time': new_dates},
                        attrs=global_attrs )
        
    elif len( arr.shape ) == 4: #(time,levels, x, y )
        # levelname to use if 4D
        if variable in ['TSLB','SMOIS']:
            levelname = 'lv_DBLY3'
        else:
            levelname = 'lv_ISBL2'

        # build dataset with levels at each timestep
        sub_ds = xr.open_dataset( sub_df.iloc[0].fn )
        ds = xr.Dataset( {variable:(['time',levelname,'x', 'y'], arr.astype(np.float32))},
                    coords={'lon': (['x', 'y'], tmp_ds[lon_variable].data),
                            'lat': (['x', 'y'], tmp_ds[lat_variable].data),
                            'time': new_dates,
                            levelname:sub_ds[ levelname ]},
                    attrs=global_attrs )
    else:
        raise BaseException( 'incorrect number of dimensions in arr. Must be 3 or 4 (as currently implemented)' )

    # set the local attrs for the given variable we are stacking
    ds[ variable ].attrs = local_attrs

    # set the lon/lat vars attrs with the existing attrs from the monthly dataset now...
    ds[ variable ].attrs = xy_attrs

    dirname, basename = os.path.split( output_filename )

    # set output compression and encoding for serialization
    encoding = ds[ variable ].encoding
    encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
    ds[ variable ].encoding = encoding

    # with open( output_path.replace('.nc', '.pickle'), 'wb' ) as ff:
    #     pickle.dump(ds, f, pickle.HIGHEST_PROTOCOL)
    #     # pkl = pickle.dumps( ds, protocol=-1 )

    # write to disk
    # remove an existing one since I think that is best practice here.
    if os.path.exists( output_filename ):
        os.remove( output_filename )

    print( 'writing to disk' )
    try:
        if not os.path.exists( dirname ):
            os.makedirs( dirname )
    except:
        pass

    ds.to_netcdf( output_filename, mode='w', format='netCDF4', engine='netcdf4' )


# # # # # EXAMPLE # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # FOR TESTING
# import os, subprocess

# input_path = '/storage01/pbieniek/gfdl/hist/hourly'
# group = 'gfdl_hist'
# variable = 'PCPT' #'T2'
# files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
# output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'
# template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable )
# years = list(range(1970,2005+1))

# os.chdir( '/workspace/UA/malindgren/repos/wrf_utils' )
# for year in years:
#     print( year )
#     output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
#     _ = subprocess.call(['python3','stack_hourly_variable_year_accumulation.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])
