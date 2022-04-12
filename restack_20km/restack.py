# pylint: disable=C0103, W0621

"""Restack the hourly data and diff/interpolate the accumulation variables

Usage:
    Designed to be called via slurm scripts created by `make_variable_sbatch_by_year.py`

    Example:
    Use these variables as args:
    # input_path_dione = '/storage01/rtladerjr/hourly'
    # input_path = '/rcs/project_data/wrf_data/hourly'
    # group = 'gfdl_rcp85'
    # group_out_name = 'GFDL-CM3_rcp85'
    # variable = 'U10'
    # year = 2008
    # files_df_fn = '/rcs/project_data/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv'
    # template_fn = '/storage01/pbieniek/gfdl/hist/monthly/monthly_PCPT-gfdlh.nc'.
    # output_filename = "/atlas_scratch/kmredilla/WRF/wind-issue/restacked/u10/U10_wrf_hourly_gfdl_rcp85_2008.nc"
"""

import argparse
import os
import multiprocessing as mp
from functools import partial
import numpy as np
import pandas as pd
import xarray as xr


# def nan_helper(y):
#     """Helper to handle indices and logical indices of NaNs.

#     Args:
#         y (numpy.ndarray): 1d array with possible NaNs
        
#     Returns:
#         - logical indices of NaNs
#         - a function, with signature indices= index(logical_indices),
#           to convert logical indices of NaNs to 'equivalent' indices
          
#     Example:
#         >>> # linear interpolation of NaNs
#         >>> nans, x= nan_helper(y)
#         >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])

#         https://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array
#     """
#     return np.isnan(y), lambda z: z.nonzero()[0]


def interp_1d_along_axis(y):
    """ interpolate across 1D timeslices of a 3D array. """
    # nans, x = nan_helper(y)
    # why do we need that function?
    nans = np.isnan(y)
    x = lambda z: z.nonzero()[0]
    y[nans] = np.interp(x(nans), x(~nans), y[~nans])
    
    return y


def open_ds(fp, variable):
    """ cleanly read variable/close a single hourly netcdf """
    with xr.open_dataset(fp) as ds:
        out = np.array(ds[variable].load()).copy()
    return out


# def rolling_window(a, window):
#     """ simple 1-D rolling window function """
#     shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
#     strides = a.strides + (a.strides[-1],)
#     return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


# def adjacent_files(df):
#     """ to be performed on the full DataFrame of pre-sorted files. """
#     # WE LOSE THE FIRST ONE IN THE SERIES...
#     adj = rolling_window(df["filepath"], window=3).tolist()
#     last = df.iloc[-2:,]["filepath"].tolist()
#     first = df.iloc[0, np.where(df.columns == "filepath")[0]].tolist()
#     # append to end of the list.
#     adj = [first] + adj + [last]

#     return adj


# def get_first_row_grouper(df):
#     """
#     after a diff (T - (T-1)) we lose the first hour. in this i grab
#     all the first layer in all the series, its adjacent files and will use
#     these with the other interpolation values of forecast time. for
#     accumulation variables (precip) only
#     """
#     years = df[df["year"] > df["year"].min()]["year"].unique()
#     years = np.sort(years[years != min(years)])
#     ind = [df[df["year"] == year].index[0] for year in years]
#     interp_list = [
#         df.iloc[[i - 1, i, i + 1]]["filepath"].tolist()
#         if i - 1 > 0
#         else df.iloc[[i, i + 1]]["filepath"].tolist()
#         for i in ind
#     ]
#     return ind, interp_list


# def stack_year(df, year, variable, ncpus=32):
#     """open and stack a single level dataset """
#     args = [(fp, variable) for fp in df[df.year == year]["filepath"]]
#     with Pool(ncpus) as pool:
#         pool.starmap(open_ds, args)

#     return out

def restack(fps, varname, ncpus):
    """Open list of hourly netCDF files, extract specified variable,
    and stack in order of provided filepath list.
    
    Args:
        fps (list): list of filepaths to extract data from and stack
        varname (str): name of variable to exrtact from hourly WRF files
        ncpus (int): number of CPUs to use with multiprocessing
        
    Returns:
        stacked_arr (numpy.ndarray): 3D array of hourly WRF outputs for a
            single variable that have been stacked along the time dimension
    """
    args = [(fp, variable) for fp in fps]
    with Pool(ncpus) as pool:
        stacked_arr = np.array(pool.starmap(open_ds, args))

    return stacked_arr


def diff_stacked(stacked_arr):
    """Wrapper for restack - open and diff a stacked array - this is
    specific processing for only the variables requiring an accumulation fix.
    
    Args:
        stacked_arr (numpy.ndarray): 3D array oof stacked WRF hourly time slices
        
    Returns:
        diff_arr (numpy.ndarray): array of differences between slices along dim 0,
            with same shape as stacked_arr
    """
    height, width = stacked_arr.shape[1:]
    diff_arr = np.concatenate(
        [np.broadcast_to(np.array([np.nan]), (1, height, width)), np.diff(stacked_arr, axis=0)]
    )
    
    return diff_arr


def restack_accum(ftimes_df, year, varname, ncpus):
    """Re-stack, diff, interp accumulation variables. """
    # group the data into forecast_time begin/end groups
    groups = ftimes_df.groupby((ftimes_df["forecast_time"] == 6).cumsum())
    # unpack to get just data frames
    groups = [df for idx, df in groups]
    # which indexes correspond to the year in question?
    ind = [count for count, df in enumerate(groups) if year in df["year"].tolist()]

    # test to be sure they are all chronological -- Should 
    #  be since dataframe is pre-sorted
    assert np.diff(ind).all() == 1

    # handle beginning and ending years in the series
    # get the forecast_time groups that overlap with our current year
    #   and the adjacent groups for seamless time-series.
    if df["year"].max() > year > df["year"].min():
        first_outer_group_idx, last_outer_group_idx = (ind[0] - 1, ind[-1] + 1)
        ind = [first_outer_group_idx] + ind + [last_outer_group_idx]

    elif year == df["year"].min():
        last_outer_group_idx = ind[-1] + 1
        ind = ind + [last_outer_group_idx]

    elif year == df["year"].max():
        first_outer_group_idx = ind[0] - 1
        ind = [first_outer_group_idx] + ind

    else:
        AttributeError("broken groups and indexing issues...")

    # get the corresponding groups for current year processing
    groups = [groups[idx] for idx in ind]

    # we need some indexing for slicing the output array to ONLY this current year
    groups_df = pd.concat(groups)  # should be chronological
    (current_year_ind,) = np.where(groups_df["year"] == year)  # along time dimension

    # # process groups and concatenate 3D cubes along time axis chronologically
    # f = partial(_run_group, variable=variable)
    # pool = mp.Pool(ncores)
    # arr = np.concatenate(pool.map(f, groups), axis=0)
    # pool.close()
    # pool.join()
    
    # process groups and concatenate 3D cubes along time axis chronologically
    stacked_arrs = [restack(df, varname, ncpus) for df in groups]
    arr = np.concatenate([diff_stacked(arr) for arr in stacked_arrs])
    # interpolate across the np.nan's brought in with differencing each forecast_time group
    arr = np.apply_along_axis(interp_1d_along_axis, axis=0, arr=arr)
    # slice back to the current year
    arr = arr[current_year_ind, ...]

    # make sure we have no leftover negative precip
    arr[arr < 0] = 0
    
    return arr


def run_restack(ftimes_df, year, varname, ncpus):
    """Run the re-stacking for a given year and variable name, handle accumulation
    variable differently if supplied 
    
    Args:
        ftimes_df (list): list of file paths to extract data from and stack
        year (int): year to run re-stacking for
        varname (str): name of variable to exrtact from hourly WRF files
        ncpus (int): number of CPUs to use with multiprocessing
    
    Returns:
        Re-stacked array!
    """

    ACCUM_VARS = ["ACSNOW", "PCPT", "PCPC", "PCPNC", "POTEVP"]

    # interpolate accumulation vars at `ind`
    if variable in ACCUM_VARS:
        arr = restack_accum(ftimes_df, year, varname)
    else:
        fps = ftimes_df[ftimes_df["year"] == year]["filepath"]
        arr = restack(fps, varname, ncpus)
    
    return arr


if __name__ == "__main__":
    # parse some args
    parser = argparse.ArgumentParser(
        description="stack the hourly raw WRF outputs to hourly NetCDF files by year."
    )
    parser.add_argument(
        "-y", "--year", action="store", dest="year", type=int, help="year to process"
    )
    parser.add_argument(
        "-v",
        "--varname",
        action="store",
        dest="varname",
        type=str,
        help="WRF variable name (exact, in file)",
    )
    parser.add_argument(
        "-f",
        "--ftimes_fp",
        action="store",
        dest="ftimes_fp",
        type=str,
        help="path to the .csv file containing parsed filename and forecast times",
    )
    parser.add_argument(
        "-t",
        "--template_fp",
        action="store",
        dest="template_fp",
        type=str,
        help="monthly template file that is used for passing global metadata to output NC files.",
    )
    parser.add_argument(
        "-o",
        "--out_fp",
        action="store",
        dest="out_fp",
        type=str,
        help="output file path for the new NetCDF hourly data for the input year",
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        type=int,
        help="Number of CPUs to use for multiprocessing",
    )
    # parse the args and unpack
    args = parser.parse_args()
    year = args.year
    varname = args.varname
    ftimes_fp = args.ftimes_fp
    template_fp = args.template_fp
    out_fp = Path(args.out_fp)
    ncpus = args.ncpus

    # wrf output standard vars -- [hardwired] for now
    lon_variable = "g5_lon_1"
    lat_variable = "g5_lat_0"

    # read in pre-built dataframe with forecast_time as a field
    ftimes_df = pd.read_csv(ftime_fp)

    # run stacking of variable through time and deal with accumulations (if needed).
    arr = run_restack(ftimes_df, year, varname, ncpus)

    # subset the data frame to the desired year -- for naming stuff
    ftimes_year_df = ftimes_df[(ftimes_df["year"] == year) & (ftimes_df["folder_year"] == year)].reset_index()

    # build the output NetCDF Dataset
    new_dates = pd.date_range(
        "-".join(ftimes_year_df.loc[ftimes_year_df.index[0], ["month", "day", "year"]].astype(str)),
        periods=arr.shape[0],
        freq="1H",
    )

    # get some template data to get some vars from... -- HARDWIRED...
    mon_tmp_ds = xr.open_dataset(template_fp, decode_times=False)
    tmp_ds = xr.open_dataset(ftimes_year_df["filepath"].tolist()[0])
    global_attrs = tmp_ds.attrs.copy()
    global_attrs["reference_time"] = str(
        new_dates[0]
    )  # 1979 hourly does NOT start at day 01...  rather day 02....
    global_attrs[
        "proj_parameters"
    ] = "+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs"
    local_attrs = tmp_ds[varname].attrs.copy()
    xy_attrs = mon_tmp_ds.lon.attrs.copy()

    if len(arr.shape) == 3:
        # build a new Dataset with the stacked timesteps and
        #   some we extracted from the input Dataset
        ds = xr.Dataset(
            {variable: (["time", "x", "y"], arr.astype(np.float32))},
            coords={
                "lon": (["x", "y"], tmp_ds[lon_variable].data),
                "lat": (["x", "y"], tmp_ds[lat_variable].data),
                "time": new_dates,
            },
            attrs=global_attrs,
        )

    elif len(arr.shape) == 4:  # (time,levels, x, y )
        # levelname to use if 4D
        if variable in ["TSLB", "SMOIS", "SH2O"]:
            levelname = "lv_DBLY3"
        else:
            levelname = "lv_ISBL2"

        # build dataset with levels at each timestep
        sub_ds = xr.open_dataset(ftimes_year_df.iloc[0]["filepath"])
        ds = xr.Dataset(
            {variable: (["time", levelname, "x", "y"], arr.astype(np.float32))},
            coords={
                "lon": (["x", "y"], tmp_ds[lon_variable].data),
                "lat": (["x", "y"], tmp_ds[lat_variable].data),
                "time": new_dates,
                levelname: sub_ds[levelname],
            },
            attrs=global_attrs,
        )
    else:
        raise BaseException(
            "incorrect number of dimensions in arr. Must be 3 or 4 (as currently implemented)"
        )

    # set the local attrs for the given variable we are stacking
    ds[varname].attrs = local_attrs

    # set the lon/lat vars attrs with the existing attrs from the monthly dataset now...
    ds[varname].attrs = xy_attrs

    # set output compression and encoding for serialization
    encoding = ds[varname].encoding
    encoding.update(
        zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32"
    )
    ds[varname].encoding = encoding

    # write to disk
    # remove an existing one since I think that is best practice here (<- original author, untested but leaving for now)
    if out_fp.exists():
        out_fp.unlink()

    ds.to_netcdf(out_fp)
    print(f"Restacked data for {varname}, {year} written to {out_fp}")
