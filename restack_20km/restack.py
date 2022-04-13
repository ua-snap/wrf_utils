# pylint: disable=C0103, W0621

"""Restack the hourly data and diff/interpolate the accumulation variables

Usage:
    Designed to be called via slurm scripts created by `restack_20km.write_sbatch_restack.py`
"""

import argparse
import os
import time
from multiprocessing import Pool
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr


def interp_1d_along_axis(y):
    """ interpolate across 1D timeslices of a 3D array. """
    nans = np.isnan(y)
    x = lambda z: z.nonzero()[0]
    y[nans] = np.interp(x(nans), x(~nans), y[~nans])
    
    return y


def open_ds(fp, varname):
    """Open a file as an xarray dataset and read
    a supplied variable's data in as an arr
    
    Args:
        fp (path_like): path to the file to open
        varname (str): name of dataset variable to read
        
    Returns:
        arr (numpy.ndarray): the data array underlying the variable of interest
    """
    with xr.open_dataset(fp) as ds:
        arr = ds[varname].values

    return arr


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
    args = [(fp, varname) for fp in fps]
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
    """Re-stack, diff, interpolate accumulation variables.
    
    Args:
        ftimes_df (pandas.DataFrame): Path to the table containing parsed filename
            and forecast times
        year (int): year being worked on
        varname (str): name of variable to extract from hourly WRF files
        ncpus (int): number of CPUs to use with multiprocessing
        
    Returns:
        arr (numpy.ndarray): 3D array of stacked, diff'd, interpolated accumulation variable data
    """
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
    if ftimes_df["year"].max() > year > ftimes_df["year"].min():
        first_outer_group_idx, last_outer_group_idx = (ind[0] - 1, ind[-1] + 1)
        ind = [first_outer_group_idx] + ind + [last_outer_group_idx]

    elif year == ftimes_df["year"].min():
        last_outer_group_idx = ind[-1] + 1
        ind = ind + [last_outer_group_idx]

    elif year == ftimes_df["year"].max():
        first_outer_group_idx = ind[0] - 1
        ind = [first_outer_group_idx] + ind

    else:
        AttributeError("broken groups and indexing issues...")

    # get the corresponding groups for current year processing
    groups = [groups[idx] for idx in ind]

    # we need some indexing for slicing the output array to ONLY this current year
    groups_df = pd.concat(groups)  # should be chronological
    (current_year_ind,) = np.where(groups_df["year"] == year)  # along time dimension
    
    # process groups and concatenate 3D cubes along time axis chronologically
    stacked_arrs = [restack(df["filepath"], varname, ncpus) for df in groups]
    arr = np.concatenate([diff_stacked(arr) for arr in stacked_arrs])
    # interpolate across the np.nan's brought in with differencing each forecast_time group
    arr = np.apply_along_axis(interp_1d_along_axis, axis=0, arr=arr)
    # slice back to the current year
    arr = arr[current_year_ind, ...]

    # make sure we have no leftover negative precip
    arr[arr < 0] = 0
    
    return arr


def rotate_winds_to_earth_coords(fp, varname, geogrid_fp):
    """
    rotate the winds data from grid-centric to earth-centric
    using file metadata that was added by P.Bieniek in the post-processed
    files given to SNAP to standardize.
    
    Args:
        fp (path_like): path to the hourly WRF data to read from and rotate
        varname (str): name of the wind variable being worked on
        geogrid_fp (path_like): path to the ancillary WRF geogrid file
        
    Returns:
        tuple of Uearth (numpy.ndarray), Vearth (numpy.ndarray), both 2D arrays of
            U and V wind components that have been rotated

    Notes:
        see http://www2.mmm.ucar.edu/wrf/users/FAQ_files/Miscellaneous.html
            for information on rotating the wind components from a WRF
            run.
    """

    if varname in ["U", "U10", "UBOT"]:
        Uvar = varname
        Vvar = varname.replace("U", "V")
    else:
        Uvar = varname.replace("V", "U")
        Vvar = varname
    # need to read both wind components to correctly rotate
    with xr.open_dataset(fp) as ds:
        Ugrid = ds[Uvar].values
        Vgrid = ds[Vvar].values

    with xr.open_dataset(geogrid_fp) as geo_ds:
        cosalpha = geo_ds["COSALPHA"].copy(deep=True)
        sinalpha = geo_ds["SINALPHA"].copy(deep=True)

    if len(Ugrid.shape) == 3:  # deal with levels
        cosalpha = np.broadcast_to(cosalpha, Ugrid.shape)
        sinalpha = np.broadcast_to(sinalpha, Ugrid.shape)

    Vearth = (Vgrid * cosalpha) + (Ugrid * sinalpha)
    Uearth = (Ugrid * cosalpha) - (Vgrid * sinalpha)

    return Uearth, Vearth


def run_rotate_winds(fp, varname, geogrid_fp):
    """Open a single wind data file and run the rotation

    Args:
        fp (path_like): path to the hourly WRF data to read from and rotate
        varname (str): name of the wind variable being worked on
        geogrid_fp (path_like): path to the ancillary WRF geogrid file

    Returns:
        arr (numpy.ndarray): the rotated wind component array for the supplied
            wind variable
        
    Since both wind components are needed to restack a single wind component, 
    this is not the most efficient when processing all wind variables,
    but it makes things a little less complicated.
    """
    ue, ve = rotate_winds_to_earth_coords(fp, varname, geogrid_fp)

    if varname in ["U", "U10", "UBOT"]:
        arr = ue
    elif varname in ["V", "V10", "VBOT"]:
        arr = ve

    return np.squeeze(np.array(arr))


def restack_winds(fps, varname, geogrid_fp, ncpus):
    """Run the stacking and rotation using multiple cores"""
    args = [(fp, varname, geogrid_fp) for fp in fps]
    with Pool(ncpus) as pool:
        stacked_arr = np.array(pool.starmap(run_rotate_winds, args))

    return stacked_arr


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
        help="WRF variable name (exact, in file)",
    )
    parser.add_argument(
        "-f",
        "--ftimes_fp",
        action="store",
        dest="ftimes_fp",
        help="path to the .csv file containing parsed filename and forecast times",
    )
    parser.add_argument(
        "-t",
        "--template_fp",
        action="store",
        dest="template_fp",
        help="monthly template file that is used for passing global metadata to output NC files.",
    )
    parser.add_argument(
        "-o",
        "--out_fp",
        action="store",
        dest="out_fp",
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
    parser.add_argument(
        "-g",
        "--geogrid_fp",
        action="store",
        dest="geogrid_fp",
        default=None,
        help="Path to ancillary WRF geogrid file.",
    )
    parser.add_argument(
        "-a",
        "--accum",
        action="store_true",
        dest="accum",
        default=False,
        help="Process suppliec variable as an accumulation variable",
    )
    # parse the args and unpack
    args = parser.parse_args()
    year = args.year
    varname = args.varname
    ftimes_fp = args.ftimes_fp
    template_fp = args.template_fp
    out_fp = Path(args.out_fp)
    ncpus = args.ncpus
    geogrid_fp = args.geogrid_fp
    accum = args.accum
    
    # safety to ensure geogrid filepath, used for wind variables, and accum flag
    #  for accumulation variables are never both specified
    assert accum != bool(geogrid_fp)
    
    # wrf output standard vars -- [hardwired] for now
    lon_variable = "g5_lon_1"
    lat_variable = "g5_lat_0"

    # read in pre-built dataframe with forecast_time as a field
    ftimes_df = pd.read_csv(ftimes_fp)

    # run stacking of variable through time and handle winds or accumulation
    #  variables as needed
    tic = time.perf_counter()
    # interpolate accumulation vars at `ind`
    if accum:
        arr = restack_accum(ftimes_df, year, varname, ncpus)
    else:
        fps = ftimes_df[ftimes_df["year"] == year]["filepath"]
        if geogrid_fp:
            arr = restack_winds(fps, varname, geogrid_fp, ncpus)
        else:
            arr = restack(fps, varname, ncpus)
    print(f"Data restacked, time elapsed: {round((time.perf_counter() - tic) / 60, 1)}m")

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
            {varname: (["time", "x", "y"], arr.astype(np.float32))},
            coords={
                "lon": (["x", "y"], tmp_ds[lon_variable].values),
                "lat": (["x", "y"], tmp_ds[lat_variable].values),
                "time": new_dates,
            },
            attrs=global_attrs,
        )

    elif len(arr.shape) == 4:  # (time,levels, x, y )
        # levelname to use if 4D
        if varname in ["TSLB", "SMOIS", "SH2O"]:
            levelname = "lv_DBLY3"
        else:
            levelname = "lv_ISBL2"

        # build dataset with levels at each timestep
        sub_ds = xr.open_dataset(ftimes_year_df.iloc[0]["filepath"])
        ds = xr.Dataset(
            {varname: (["time", levelname, "x", "y"], arr.astype(np.float32))},
            coords={
                "lon": (["x", "y"], tmp_ds[lon_variable].values),
                "lat": (["x", "y"], tmp_ds[lat_variable].values),
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
    
    tic = time.perf_counter()
    ds.to_netcdf(out_fp)

    print((
        f"Restacked data for {varname}, {year} written to {out_fp} at {time.ctime()}, "
        f"time elapsed: {round((time.perf_counter() - tic) / 60, 1)}m"
    ))
