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

def interp_file_grouper( df, reinit_day_value=6 ):
    ''' return filenames for linear interpolation. format:(t-1, t, t+1) '''
    ind, = np.where( df.forecast_time == reinit_day_value )
    interp_list = [ df.iloc[[i-1,i,i+1]].fn.tolist() if i-1 > 0 
                    else df.iloc[[i,i+1]].fn.tolist() for i in ind ]
    return interp_list

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

def _interpolate_hour( x ):
    ''' 
    interpolate over a missing hour using hours on each side 
    this is only applicable for the accumulation (precip) vars
    x is a list of 3 adjacent filenames where the middle one will 
    be filled linearly with the 2 ends.
    '''
    x, variable = x
    arr = np.array([ open_ds(fn, variable) for fn in x ])
    arr[ 1 ] = np.nan
    out = np.apply_along_axis( 
        interp_1d_along_axis, axis=0, arr=arr.copy() )
    return out

def _update_arr( arr, key, value ):
    ''' helper to update an array in a listcomp '''
    arr[ key, ... ] = value
    return 1

def _run_interp( x, variable ):
    i, j = x
    return i, _interpolate_hour((j, variable))[1,...].copy()

def run_year( df, variable ):
    import numpy as np
    import multiprocessing as mp
    from functools import partial
    DIFF_VARS = ['PCPT']

    # stack the data along time axis
    arr = stack_year( df, variable )

    # interpolate accumulation vars at `ind`
    if variable in DIFF_VARS:
        # index layers requiring interpolation
        ind, = np.where( df.interp == True )
        # NOTE AND FIX NEEDED: 
        # it will be necessary to either add the index that is lost in the diff'ing
        # to the list of files to be interpolated OR we needto just add back some
        # layer we already have. Currently we are using a copy of missing_layer+1
        print( 'interpolating missing hours --> {}'.format( variable ) )
        # diff it (T - (T-1)) [reverse array along time axis before and after diff]
        # diff_arr = np.diff( arr[::-1,...], axis=0 )[::-1,...] # double flip!
        # NO REVERSING!!! THE np.diff does it that way too...
        diff_arr = np.diff( arr, axis=0 ) # NO FLIP!
        # add the missing file back to the beginning
        # replicate the first timestep -- hour2 to return hour1 lost in diffing
        diff_arr = np.concatenate( [arr[0,...][np.newaxis, ...], diff_arr], axis=0 )
        arr = diff_arr.copy()
        del diff_arr
    
        # multiprocess interpolation
        f = partial( _run_interp, variable=variable )
        col_ind, = np.where( df.columns == 'interp_files' )
        pool = mp.Pool( 32 )
        interpolated = pool.map( f, zip(ind, df.iloc[ind, col_ind]['interp_files'].tolist()) )
        pool.close()
        pool.join()

        # put the data back into the arr
        _ = [ _update_arr( arr, key, value ) for key, value in interpolated ]

        # Make sure we don't have any negative precip and set it to 0 if so (it happens) 
        arr[ (arr < 0) | (arr == np.nan) ] = 0
    return arr

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

    # # FOR TESTING
    # input_path = '/storage01/pbieniek/gfdl/hist/hourly'
    # group = 'gfdl_hist'
    # variable = 'PCPT' #'T2'
    # files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    # output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/v2'
    # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_{}-gfdlh.nc'.format( variable )
    # year = 1990
    # # END TESTING

    # wrf output standard vars -- [hardwired] for now
    lon_variable = 'g5_lon_1'
    lat_variable = 'g5_lat_0'

    # read in pre-built dataframe with forecast_time as a field
    df = pd.read_csv( files_df_fn, sep=',', index_col=0 )

    # # sort it
    # df = df.sort_values( ['year', 'month', 'day', 'hour'] ).reset_index()

    # set vars based on whether to interp
    ind, = np.where( df.forecast_time == 6 )
    df[ 'interp' ] = False
    df.iloc[ ind, np.where(df.columns == 'interp')[0] ] = True
    interp_list = interp_file_grouper( df )
    df[ 'interp_files' ] = [ [i] for i in df.fn ]
    df.iloc[ ind, np.where(df.columns == 'interp_files')[0] ] = interp_list

    # # grab data for the requeseted year
    sub_df = df[ (df.year == year) & (df.folder_year == year) ].reset_index()
    # fill an interp files for T1 if not the first year
    if year > df.year.min():
        sub_df.iloc[0, np.where(sub_df.columns == 'interp')[0]] = True
        ii, = df[ df.fn == sub_df.iloc[0].fn ].index
        interp_list1 = df.iloc[[ii-1, ii, ii+1]].fn.tolist()
        # set the list the hard way...
        sub_df = sub_df.set_value( 0, 'interp_files', interp_list1 )

    # run the stacking
    arr = run_year( sub_df, variable )

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

    # build a new Dataset with the stacked timesteps and some we extracted from the input Dataset
    ds = xr.Dataset( {variable:(['time','x', 'y'], arr)},
                    coords={'lon': (['x', 'y'], tmp_ds[lon_variable].data),
                            'lat': (['x', 'y'], tmp_ds[lat_variable].data),
                            'time': new_dates},
                    attrs=global_attrs )

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
#     _ = subprocess.call(['python3','stack_hourly_variable_year_with_interpfix.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])
