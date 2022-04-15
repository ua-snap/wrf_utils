# pylint: disable=C0103, W0621

"""stack the hourly data and diff/interpolate the accumulation variables
This script is specifically for stacking the wind variables.

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
from datetime import datetime
from functools import partial
import pandas as pd
import numpy as np
import xarray as xr


def rotate_winds_to_earth_coords(fn, variable, ancillary_fn):
    """
    rotate the winds data from grid-centric to earth-centric
    using file metadata that was added by P.Bienik in the post-processed
    files given to SNAP to standardize.

    Notes:
        see http://www2.mmm.ucar.edu/wrf/users/FAQ_files/Miscellaneous.html
            for information on rotating the wind components from a WRF
            run.
    """

    if variable in ["U", "U10", "UBOT"]:
        Uvar = variable
        Vvar = variable.replace("U", "V")
    elif variable in ["V", "V10", "VBOT"]:
        Uvar = variable.replace("V", "U")
        Vvar = variable
    else:
        AttributeError("must have a U or V in the name -- WINDS only")

    with xr.open_dataset(fn) as ds:
        Ugrid = ds[Uvar].data
        Vgrid = ds[Vvar].data

    with xr.open_dataset(ancillary_fn) as ancillary:
        cosalpha = ancillary["COSALPHA"].copy(deep=True)
        sinalpha = ancillary["SINALPHA"].copy(deep=True)

    ancillary = None  # cleanup

    if len(Ugrid.shape) == 3:  # deal with levels
        cosalpha = np.broadcast_to(cosalpha, Ugrid.shape)
        sinalpha = np.broadcast_to(sinalpha, Ugrid.shape)

    Vearth = (Vgrid * cosalpha) + (Ugrid * sinalpha)
    Uearth = (Ugrid * cosalpha) - (Vgrid * sinalpha)

    # DO NOT USE THIS VERSION WITH THE geo_em.d1.nc file
    # Vearth = (cosalpha*Vgrid) - (sinalpha*Ugrid)
    # Uearth = (sinalpha*Vgrid) + (cosalpha*Ugrid)

    return Uearth, Vearth


def run_winds(fn, variable, ancillary_fn):
    """Open a single wind data file and run the rotation"""
    ue, ve = rotate_winds_to_earth_coords(fn, variable, ancillary_fn)

    if variable in ["U", "U10", "UBOT"]:
        out = ue
    elif variable in ["V", "V10", "VBOT"]:
        out = ve

    return np.squeeze(np.array(out))


def stack_year_wind_rot(df, year, variable, ancillary_fn, ncores=32):
    """Run the stacking and rotation using multiple cores"""
    f = partial(run_winds, variable=variable, ancillary_fn=ancillary_fn)
    files = df[df.year == year].fn.tolist()

    pool = mp.Pool(ncores)
    out = pool.map(f, files)
    pool.close()
    pool.join()
    arr = np.array(out)

    return arr


def fix_input_df_pathnames(df, input_path, input_path_dione):
    """
    this is a way to update the input_path of the current year to the faster /atlas_scratch
    input location that we are using now.
    """
    df["fn"] = df.fn.apply(lambda x: x.replace(input_path_dione, input_path))

    return df


def run_year(df, year, variable, ancillary_fn=None):
    """Run the winds for a given year,
    This script should only be called when working on wind variables
    """
    WIND_VARS = ["U", "U10", "UBOT", "V", "V10", "VBOT"]

    # interpolate accumulation vars at `ind`
    if variable in WIND_VARS:
        if ancillary_fn:
            arr = stack_year_wind_rot(df, year, variable, ancillary_fn)
        else:
            BaseException(
                "please add a path for ancillary_fn when working with winds stacking."
            )

    return arr


if __name__ == "__main__":
    # parse some args
    parser = argparse.ArgumentParser(
        description="stack the hourly outputs from raw WRF outputs to NetCDF files by year."
    )
    parser.add_argument(
        "-i",
        "--input_path",
        action="store",
        dest="input_path",
        type=str,
        help="input hourly directory with annual sub-dirs containing raw WRF NetCDF outputs",
    )
    parser.add_argument(
        "-id",
        "--input_path_dione",
        action="store",
        dest="input_path_dione",
        type=str,
        help="directory on Dione server with annual sub-dirs of raw hourly WRF output",
    )
    parser.add_argument(
        "-y", "--year", action="store", dest="year", type=int, help="year to process"
    )
    parser.add_argument(
        "-f",
        "--files_df_fn",
        action="store",
        dest="files_df_fn",
        type=str,
        help="path to the .csv file containing parsed filename and forecast_time",
    )
    parser.add_argument(
        "-v",
        "--variable",
        action="store",
        dest="variable",
        type=str,
        help="variable name (exact)",
    )
    parser.add_argument(
        "-o",
        "--output_filename",
        action="store",
        dest="output_filename",
        type=str,
        help="output filename for the new NetCDF hourly data for the input year",
    )
    parser.add_argument(
        "-t",
        "--template_fn",
        action="store",
        dest="template_fn",
        type=str,
        help="monthly template file that is used for passing global metadata to output NC files.",
    )
    parser.add_argument(
        "-a",
        "--ancillary_fn",
        action="store",
        dest="ancillary_fn",
        type=str,
        help="ancillary file with COSALPHA, SINALPHA needed for rotation to Earth coordinates.",
    )

    # parse the args and unpack
    args = parser.parse_args()
    input_path = args.input_path
    input_path_dione = args.input_path_dione
    year = args.year
    files_df_fn = args.files_df_fn
    variable = args.variable
    output_filename = args.output_filename
    template_fn = args.template_fn
    ancillary_fn = args.ancillary_fn

    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"working on {year} {variable}, {current_time}")
    # wrf output standard vars -- [hardwired] for now
    lon_variable = "g5_lon_1"
    lat_variable = "g5_lat_0"

    # read in pre-built dataframe with forecast_time as a field
    df = pd.read_csv(files_df_fn, sep=",", index_col=0).copy()

    # [ TEMPORARY FIX ] fix the filenames in the DF
    # this is due to running from /atlas_scratch instead of dione for speed
    df = fix_input_df_pathnames(df, input_path, input_path_dione)

    # run stacking of variable through time and deal with accumulations (if needed).
    arr = run_year(df, year, variable, ancillary_fn)

    # subset the data frame to the desired year -- for naming stuff
    sub_df = df[(df.year == year) & (df.folder_year == year)].reset_index()

    # build the output NetCDF Dataset
    new_dates = pd.date_range(
        "-".join(sub_df.loc[sub_df.index[0], ["month", "day", "year"]].astype(str)),
        periods=arr.shape[0],
        freq="1H",
    )

    # get some template data to get some vars from... -- HARDWIRED...
    mon_tmp_ds = xr.open_dataset(template_fn, decode_times=False)
    tmp_ds = xr.open_dataset(sub_df.fn.tolist()[0])
    global_attrs = tmp_ds.attrs.copy()
    global_attrs["reference_time"] = str(
        new_dates[0]
    )  # 1979 hourly does NOT start at day 01...  rather day 02....
    global_attrs[
        "proj_parameters"
    ] = "+units=m +proj=stere +lat_ts=64.0 +lon_0=-152.0 +lat_0=90.0 +x_0=0 +y_0=0 +a=6370000 +b=6370000"
    local_attrs = tmp_ds[variable].attrs.copy()
    xy_attrs = mon_tmp_ds.lon.attrs.copy()

    if len(arr.shape) == 3:
        # build a new Dataset with the stacked timesteps and
        # some we extracted from the input Dataset
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
        sub_ds = xr.open_dataset(sub_df.iloc[0].fn)
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
    ds[variable].attrs = local_attrs

    # set the lon/lat vars attrs with the existing attrs from the monthly dataset now...
    ds[variable].attrs = xy_attrs

    dirname, basename = os.path.split(output_filename)

    # set output compression and encoding for serialization
    encoding = ds[variable].encoding
    encoding.update(
        zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32"
    )
    ds[variable].encoding = encoding

    # write to disk
    # remove an existing one since I think that is best practice here.
    if os.path.exists(output_filename):
        os.remove(output_filename)
    elif not os.path.exists(dirname):
        os.makedirs(dirname)

    print(f"writing {output_filename}")

    ds.to_netcdf(output_filename, mode="w", format="netCDF4", engine="netcdf4")
