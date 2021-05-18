"""For resampling the hourly data to daily data for 
a select subset of variables. 
"""


def make_args(base_path, variables=None, agg_group="hourly"):
    wildcard = "*.nc"
    if variables is None:
        variables = [
            os.path.basename(i)
            for i in glob.glob(os.path.join(base_path, agg_group, "*"))
            if os.path.isdir(i) and "slurm" not in i
        ]
    args = []
    for variable in variables:
        files = sorted(
            glob.glob(os.path.join(base_path, agg_group, variable.lower(), wildcard))
        )
        varnames = [variable] * len(files)
        args = args + list(zip(varnames, files))
    return args


def resample(fn, variable, agg_str="D"):
    print("working on: {} - {}".format(fn, variable))

    ds = xr.open_dataset(fn)
    # get some attrs
    global_attrs = ds.attrs
    local_attrs = ds[variable].attrs
    time_attrs = ds.time.attrs

    # pathing
    output_path = os.path.join(base_path, "daily", variable.lower())

    try:
        if not os.path.exists(output_path):
            os.makedirs(output_path)
    except:
        pass  # since it is a parallel op

    # metric switch -- aggregation
    if variable in [
        "PCPT",
        "pcpt",
        "PCPC",
        "pcpc",
        "PCPNC",
        "pcpnc",
        "ACSNOW",
        "acsnow",
    ]:
        metric = "sum"
        ds_day = ds.resample(time=agg_str).sum()
    else:
        metric = "mean"
        ds_day = ds.resample(time=agg_str).mean()

    ds_day_comp = ds_day.compute()  # watch this one
    local_attrs.update(
        temporal_resampling="Daily: these data represent {} daily outputs\
                         from hourly wrf dynamical downscaling outputs.".format(
            metric
        )
    )

    # ds_day_comp = ds_day_comp.to_dataset( name=variable )
    ds_day_comp.attrs = global_attrs
    ds_day_comp[variable].attrs = local_attrs
    ds_day_comp["time"].attrs = time_attrs

    # set output compression and encoding for serialization
    encoding = ds_day_comp[variable].encoding
    encoding.update(
        zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype="float32"
    )
    ds_day_comp[variable].encoding = encoding

    # dump to disk
    dirname, basename = os.path.split(fn)
    # [ WATCH ] this is hardwired to hourly / daily...
    basename = basename.replace(
        "hourly", "daily"
    )  # .replace( '.nc', '_{}.nc'.format(metric) )
    output_filename = os.path.join(output_path, basename)

    ds_day_comp.to_netcdf(output_filename, mode="w", format="NETCDF4_CLASSIC")

    # cleanup file handles
    ds.close()
    ds = None
    ds_day.close()
    ds_day = None
    ds_day_comp.close()
    ds_day_comp = None

    # using the base netCDF4 package update the times to be UTC and dump back to disk
    # hacky but overcomes a current somewhat limitation in xarray.
    # out_fn = force_update_times_UTC( output_filename )
    out_fn = output_filename

    return out_fn


def wrap(x):
    variable, fn = x
    return resample(fn, variable)


def force_update_times_UTC(fn):
    """ use the base netCDF4 python pkg to see if we can add UTC to times."""
    import netCDF4 as nc4

    # open the dataset with xarray to cut down on code
    with xr.open_dataset(fn) as ds:
        dates = ds.time.to_index().to_pydatetime().tolist()

    ds = None

    f = nc4.Dataset(fn, mode="r+")
    units = f["time"].units
    suffix = " UTC"
    if not units.endswith(suffix):
        units = units + suffix

    # units = 'hours since 1979-01-02 00:00 UTC'
    values = nc4.date2num(dates, units=units)
    f["time"][:] = values
    f["time"].units = units

    f.close()
    return fn


if __name__ == "__main__":
    # resample hourly to monthly for data delivery
    import xarray as xr
    import numpy as np
    import pandas as pd
    import os, glob, itertools
    import multiprocessing as mp

    # base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data'
    base_path = "/storage01/malindgren/wrf_ccsm4"
    # variables = ['pcpt','t2',] # 't2min','t2max'] # from downscaling team regarding the dailies on AWS
    variables = [
        "q2",
        "pcpc",
    ]

    # # # # # # # # # # # # # # # # # #
    # # VARIABLES FOR DAILY FOLDER AWS
    # # --- --- --- --- --- --- --- ---
    # Tmax
    # Tmin
    # Tmean
    # Precipitation
    # Winds (speed and direction)
    # Humidity
    # # # # # # # # # # # # # # # # # #

    args = make_args(base_path, variables=variables, agg_group="hourly_fix")
    # args = [(i,j) for i,j in args if 'historical' in j ]
    ncpus = 20

    # parallel process
    pool = mp.Pool(ncpus)
    out = pool.map(wrap, args)
    pool.close()
    pool.join()
