#! ~/v3/bin/python3

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
# stack the hourly data and diff/interpolate the accumulation variables
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

def get_month_day( fn ):
    ''' split WRF output filenames from UAF group and split to descriptor parts '''
    dirname, basename = os.path.split( fn )
    year, month, day_hour = basename.split('.')[-2].split('-')
    day, hour = day_hour.split( '_' )
    folder_year = dirname.split('/')[-1]
    return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}

def open_ds( fn, variable ):
    ''' cleanly read variable/close a single hourly netcdf '''
    import xarray as xr
    ds = xr.open_dataset( fn, autoclose=True )
    out = ds[ variable ].copy()
    ds.close()
    return out.data

# def interp_file_grouper( df, reinit_day_value=6 ):
#     ''' return filenames for linear interpolation. format:(t-1, t, t+1) '''
#     ind, = np.where( df.forecast_time == reinit_day_value )
#     interp_list = [ df.iloc[[i-1,i,i+1]].fn.tolist() if i-1 > 0 
#                     else df.iloc[[i,i+1]].fn.tolist() for i in ind ]
#     return interp_list

# def interp_file_grouper2( df ):
#     ''' return filenames for linear interpolation. format:(t-1, t, t+1) '''
#     rows, cols = df.shape
#     indexes = list(range(rows))
#     interp_list = [ df.iloc[[i-1,i,i+1]].fn.tolist() for i in indexes[1:-1] ]
#     last_idx = indexes[-1]
#     # add back the first one and put the final as only 2 filenames...
#     interp_list = [[df.iloc[0].fn]] + interp_list + [df.iloc[[last_idx-1,last_idx]].fn.tolist()]
#     return interp_list

def rolling_window( a, window ):
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

def stack_year( df, variable ):
    ''' open and stack a single level dataset '''
    import multiprocessing as mp
    from functools import partial
    
    pool = mp.Pool( 32 )
    f = partial(open_ds, variable=variable)
    out = np.array(pool.map( f, df.fn.tolist() ))
    pool.close()
    pool.join()
    return out

def open_all_year( df, variable ):
    ''' open and stack a single level dataset '''
    import multiprocessing as mp
    from functools import partial
    
    pool = mp.Pool( 32 )
    f = partial(open_ds, variable=variable)
    out = pool.map( f, df.fn.tolist() )
    pool.close()
    pool.join()
    return out

def open_diff_interp( files, variable, interp=False ):
    if interp and len(files) == 3:
        # there _must_ be 3 files to interp...    
        arr = _interpolate_hour( files )
        diff
    else:
        # just diff em
        # grab the first 2 files as Tminus1 and T (respectively)
        T_minus1 = open_ds( files[0], variable )
        T = open_ds( files[1], variable )

        arr = T - T_minus1
    return arr

def stack_year_accum( df, variable ):
    ''' open and stack a single level accumulated dataset '''
    import multiprocessing as mp
    from functools import partial
    
    pool = mp.Pool( 32 )
    f = partial( open_diff, variable=variable )
    out = np.array( pool.map( f, df.interp_files.tolist() ) )
    pool.close()
    pool.join()
    return out

def _interpolate_hour( x ):
    ''' 
    interpolate over a missing hour using hours on each side 
    this is only applicable for the accumulation (precip) vars
    x is a list of 3 adjacent filenames where the middle one will 
    be filled linearly with the 2 ends.
    '''
    x, variable = x
    arr = np.array([ open_ds(fn, variable) for fn in x ])
    time, lat, lon = arr.shape

    arr[ 1, ... ] = np.nan
    out = np.apply_along_axis( 
        interp_1d_along_axis, axis=0, arr=arr.copy() )
    return out

def _interpolate_hours( arr, ind ):
    ''' 
    interpolate over a missing hour using hours on each side 
    this is only applicable for the accumulation (precip) vars
    x is a list of 3 adjacent filenames where the middle one will 
    be filled linearly with the 2 ends.
    '''
    # set the forecast=6 slices to np.nan for linear interpolation
    arr[ ind, ... ] = np.nan
    return np.apply_along_axis( 
        interp_1d_along_axis, axis=0, arr=arr )

def _update_arr( arr, key, value ):
    ''' helper to update an array in a listcomp '''
    arr[ key, ... ] = value
    return 1

def _run_interp( x, variable ):
    i, j = x
    return i, _interpolate_hour((j, variable))[1,...].copy()

def run_year( sub_df, variable ):
    import numpy as np
    import multiprocessing as mp
    from functools import partial
    import glob

    # ACCUMULATION VARIABLE LIST... WE HANDLE THESE DIFFERENTLY THAN OTHER VARS...
    DIFF_VARS = [ 'PCPT', 'ACSNOW', 'PCPT', 'PCPC', 'PCPNC', 'POTEVP' ]

    # interpolate accumulation vars at `ind`
    if variable in DIFF_VARS:
        arr = stack_year_accum( sub_df, variable )
        # index layers requiring interpolation
        ind, = np.where( sub_df.interp == True )
        ind, = np.where( sub_df.forecast_time == 6 )
        print( 'interpolating missing hours --> {}'.format( variable ) )
        
        # build some potential paths for searching if its the first year in the series. interpolation stuff.
        dirname = os.path.dirname( sub_df.fn.tolist()[1] )
        year = dirname.split(os.path.sep)[-1]
        path = os.path.join( os.path.dirname(dirname), str( int(year)+1 ) )
        
        # if os.path.exists( path ):
        #     # read in the last file of that years series year-12-31 23
        #     last_fn = sorted( glob.glob( os.path.join(path, '*.nc')))[-1]
        #     last_arr = open_ds( last_fn, variable )
        #     arr = np.concatenate([last_arr[np.newaxis, ...], arr], axis=0) # add the last ds
        #     diff_arr = np.diff( arr, axis=0 )
        # else:
        #     # must be the first in the series and there is no way we can fill 
        #     # in the first missing value in the required np.diff
        #     diff_arr = np.diff( arr, axis=0 )

        #     # add the missing file back to the beginning
        #     # replicate the first timestep -- hour2 to return hour1 lost in diffing
        #     # diff_arr = np.concatenate( [arr[0,...][np.newaxis, ...], diff_arr], axis=0 )

        #     # # make a dummy first timestep that is all ZERO for now.
        #     diff_arr = np.concatenate( [np.zeros_like(arr[0,...][np.newaxis, ...]), diff_arr], axis=0 )
        
        # diff_arr = np.concatenate( [np.zeros_like(arr[0,...][np.newaxis, ...]), diff_arr], axis=0 )

        # arr = diff_arr.copy()
        # arr = _interpolate_hours( diff_arr, ind ) # this is the _other_ way to do it.
        # del diff_arr

        # multiprocess interpolation
        f = partial( _run_interp, variable=variable )
        col_ind, = np.where( sub_df.columns == 'interp_files' )
        pool = mp.Pool( 32 )
        interpolated = pool.map( f, zip(ind, sub_df.iloc[ind, col_ind]['interp_files'].tolist()) )
        pool.close()
        pool.join()

        # put the data back into the arr
        _ = [ _update_arr( arr, key, value ) for key, value in interpolated ]

        # Make sure we don't have any negative precip and set it to 0 if so (it happens) 
    else:
        # stack the data along time axis
        arr = stack_year( sub_df, variable )
        arr[ (arr < 0) ] = 0
    return arr

def _run_group( group, variable ):
    '''
    open and diff within a single forecast_time group through forecast_time=6
    
    this is specific processing for only the variables requiring an accumulation fix
    
    '''
    arr = np.array([open_ds(i, variable) for i in group.fn])
    time, height, width = arr.shape
    diff_arr = np.concatenate([ np.broadcast_to( np.array([np.nan]), (1,height,width)), np.diff( arr, axis=0 ) ])
    return diff_arr

if __name__ == '__main__':
    import os
    import xarray as xr
    import numpy as np
    import pandas as pd
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
    # input_path = '/storage01/pbieniek/gfdl/hist/hourly'
    # group = 'gfdl_hist'
    # variable = 'PCPT' #'T2'
    # files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    # output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'
    # # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable )
    # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( 'PCPT' )
    # output_filename = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2/PCPT_wrf_hourly_gfdl_hist_1990_FULLTEST2.nc'
    # year = 1990
    # # # # # END TESTING

    # wrf output standard vars -- [hardwired] for now
    lon_variable = 'g5_lon_1'
    lat_variable = 'g5_lat_0'

    # read in pre-built dataframe with forecast_time as a field
    df = pd.read_csv( files_df_fn, sep=',', index_col=0 )
    # df[ 'interp_files' ] = adjacent_files( df )

    # # # TEST WITH GROUPBY FORECAST_TIME INTERVALS
    from pathos.mp_map import mp_map
    from functools import partial

    # group the data into forecast_time begin/end groups
    groups = df.groupby((df.forecast_time == 6).cumsum())
    # unpack it.  this is ugly, but I really hate pandas apply.
    groups = [ group for idx,group in groups ]
    # which indexes correspond to the year in question?
    ind = [ count for count, group in enumerate( groups ) if year in group.year.tolist() ]
    
    # test to be sure they are all chronological -- Should be since dataframe is pre-sorted
    assert np.diff(ind).all() == 1

    # if it is not the first year in the whole series, then grab the last years' final file
    # for differencing... This is something that will need to be thought through a bit more fully... 
    if year > df.year.min() and year < df.year.max():
        # get the forecast_time groups that overlap with our current year
        #   and the adjacent 2 groups for smoothness.
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
           
    # process groups and concatenate along time axis
    f = partial( _run_group, variable=variable )
    arr = np.concatenate( mp_map( f, groups, nproc=25 ), axis=0 )

    # interpolate across the np.nan's brought in with differencing each forecast_time group
    arr = np.apply_along_axis( interp_1d_along_axis, axis=0, arr=arr )

    # slice back to the current year
    arr = arr[ current_year_ind, ... ]

    # make sure we have no leftover negative precip
    arr[ arr < 0 ] = 0

    # # # # # END TEST  # # # # # # #

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
        ds = xr.Dataset( {variable:(['time','x', 'y'], arr)},
                        coords={'lon': (['x', 'y'], tmp_ds[lon_variable].data),
                                'lat': (['x', 'y'], tmp_ds[lat_variable].data),
                                'time': new_dates},
                        attrs=global_attrs )
        
    elif len( arr.shape ) == 4: #(time,levels, x, y )
        # levelname to use if 4D
        if variable in ['TSLB']:
            levelname = 'lv_DBLY3'
        else:
            levelname = 'lv_ISBL2'

        # build dataset with levels at each timestep
        sub_ds = xr.open_dataset( sub_df.iloc[0].fn )
        ds = xr.Dataset( {variable:(['time',levelname,'x', 'y'], arr)},
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
    # write to disk
    print( 'writing to disk' )
    try:
        # final_path = os.path.join( dirname, variable.lower() )
        if not os.path.exists( dirname ):
            os.makedirs( dirname )
    except:
        pass

    # output_filename = os.path.join( dirname, '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
    ds.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )


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
#     output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
#     _ = subprocess.call(['python3','stack_hourly_variable_year_accumulation.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])
