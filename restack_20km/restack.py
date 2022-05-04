# pylint: disable=C0103, W0621

"""Restack the hourly data and diff/interpolate the accumulation variables

Usage:
    Designed to be called via slurm scripts created by `restack_20km.write_sbatch_restack.py`
"""

import argparse
import datetime
import importlib.util
import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from pyproj import Proj, Transformer


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
        [
            np.broadcast_to(np.array([np.nan]), (1, height, width)),
            np.diff(stacked_arr, axis=0),
        ]
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


def derive_xy(geogrid_fp, wrf_proj_str):
    """Derive the x and y coordinate axes for the WRF grid
    
    Args:
        geogrid_fp (path_like): path to the WRF geogrid file
        wrf_proj_str (str): Proj4 string for the WRF grid derived in
            ancillary/include_latlon.ipynb
    
    Returns:
        tuple of x and y arrays containing x and y coordinate values 
    """
    wrf_proj = Proj(wrf_proj_str)
    wgs_proj = Proj(proj="latlong", datum="WGS84")
    transformer = Transformer.from_proj(wgs_proj, wrf_proj)

    with xr.open_dataset(geogrid_fp) as geo_ds:
        # Easting and Northings of the domains center point
        e, n = transformer.transform(geo_ds.CEN_LON, geo_ds.CEN_LAT)
        dx, dy = geo_ds.DX, geo_ds.DY
        nx, ny = geo_ds.dims["west_east"], geo_ds.dims["south_north"]

    # Down left corner of the domain
    x0 = -(nx - 1) / 2.0 * dx + e
    y0 = -(ny - 1) / 2.0 * dy + n
    x = np.arange(nx) * dx + x0
    # flip the y-axis to match flipped lat/lon grids and flipped data array
    y = np.flip(np.arange(ny) * dy + y0)

    return x, y


def path_import(module_fp):
    """Import a module given its path. Intended for loading luts.py.
    
    Args:
        module_fp (path_like): path to the module to import
    
    Returns:
        a module object created from the module at path in module_fp
    """
    module_name = module_fp.name.split(".")[0]
    spec = importlib.util.spec_from_file_location(module_name, module_fp)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module


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
        "-o",
        "--out_fp",
        action="store",
        dest="out_fp",
        help="output file path for the new NetCDF hourly data for the input year",
    )
    parser.add_argument(
        "-l",
        "--luts_fp",
        action="store",
        dest="luts_fp",
        help="Path to luts.py file for the restack_20km pipeline",
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
    # parse the args and unpack
    args = parser.parse_args()
    year = args.year
    varname = args.varname
    ftimes_fp = args.ftimes_fp
    out_fp = Path(args.out_fp)
    luts_fp = Path(args.luts_fp)
    ncpus = args.ncpus
    geogrid_fp = args.geogrid_fp

    # import the luts table supplied as a path
    luts = path_import(luts_fp)

    # read in pre-built dataframe with forecast_time as a field
    ftimes_df = pd.read_csv(ftimes_fp)

    # run the re-stacking of data through time, and handle winds or accumulation
    #  variables as needed
    tic = time.perf_counter()
    # interpolate accumulation vars at `ind`
    if varname in luts.accum_varnames:
        arr = restack_accum(ftimes_df, year, varname, ncpus)
    else:
        fps = ftimes_df[ftimes_df["year"] == year]["filepath"]
        if varname in luts.wind_varnames:
            arr = restack_winds(fps, varname, geogrid_fp, ncpus)
        else:
            arr = restack(fps, varname, ncpus)
    print(
        f"Data restacked, time elapsed: {round((time.perf_counter() - tic) / 60, 1)}m"
    )

    # subset the data frame to the desired year -- for naming stuff
    ftimes_year_df = ftimes_df[
        (ftimes_df["year"] == year) & (ftimes_df["folder_year"] == year)
    ].reset_index()

    # build the output NetCDF Dataset
    # pull time stamp values from table instead of pandas.date_range()
    # that would have to be corrected later
    new_dates = pd.DatetimeIndex(
        [
            datetime.datetime(
                *[row[colname] for colname in ["year", "month", "day", "hour"]]
            )
            for i, row in ftimes_year_df.iterrows()
        ]
    )

    tmp_fp = ftimes_year_df["filepath"].iloc[0]
    with xr.open_dataset(tmp_fp) as tmp_ds:
        global_attrs = tmp_ds.attrs
        local_attrs = tmp_ds[varname].attrs
        encoding = tmp_ds[varname].encoding
        lon_arr = tmp_ds[luts.lon_variable].values
        lat_arr = tmp_ds[luts.lat_variable].values
        # get level name if present, should always be the first one
        if len(tmp_ds[varname].dims) == 3:
            levelname = tmp_ds[varname].dims[0]
            levels = tmp_ds[levelname].values
        else:
            levelname = None

    global_attrs.update(luts.global_attrs)
    global_attrs["reference_time"] = str(new_dates[0])

    x, y = derive_xy(geogrid_fp, global_attrs["proj_parameters"])
    # build dicts for creating xarray dataset:
    coords_dict = {
        # flip the lon and lat arrays to provide more intuitive orientation
        #  within dataset
        "time": (["time"], new_dates, luts.coord_attrs["time"]),
        # "xc" and "yc" naming retained from original restacking pipeline
        "yc": (["yc"], y, luts.coord_attrs["yc"]),
        "xc": (["xc"], x, luts.coord_attrs["xc"]),
        "lon": (["yc", "xc"], np.flipud(lon_arr), luts.coord_attrs["lon"]),
        "lat": (["yc", "xc"], np.flipud(lat_arr), luts.coord_attrs["lat"]),
    }

    # handle variables with a 4th dim level
    if levelname is not None:
        new_levelname = luts.levelnames[levelname]
        dims = ["time", new_levelname, "yc", "xc"]
        coords_dict[new_levelname] = [
            [new_levelname],
            levels,
            luts.var_attrs[levelname],
        ]
    else:
        dims = ["time", "yc", "xc"]

    crs_varname = "polar_stereographic"
    local_attrs["grid_mapping"] = crs_varname
    wrf_proj = Proj(global_attrs["proj_parameters"])
    new_varname = varname.lower()
    data_dict = {
        # add the data flipped along y axis to match flipped lat/lon arrays
        #  and y-coordinate array
        new_varname: (dims, np.flip(arr.astype(np.float32), axis=1), local_attrs),
        # create CRS variable and add info
        crs_varname: ([], np.array(b""), wrf_proj.crs.to_cf()),
    }
    ds = xr.Dataset(data_dict, coords_dict, global_attrs)

    # set output compression and encoding for serialization
    encoding.update(
        zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32"
    )
    ds[new_varname].encoding = encoding

    # write to disk
    # remove an existing one since I think that is best practice
    #   here (<- original author, untested but leaving for now)
    if out_fp.exists():
        out_fp.unlink()

    tic = time.perf_counter()
    ds.to_netcdf(out_fp, engine="netcdf4")
    print(
        (
            f"Restacked data for {varname}, {year} written to {out_fp} at {time.ctime()}, "
            f"time elapsed: {round((time.perf_counter() - tic) / 60, 1)}m"
        )
    )
