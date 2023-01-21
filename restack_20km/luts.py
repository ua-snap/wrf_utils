"""Lookup tables for re-stacking the WRF data."""

import numpy as np


# a lookup for mapping the WRF groups (model/scenario)
# to other useful info.
groups = {
    "erain_hist": {
        "years": np.arange(1979, 2019),
        "fn_str": "ERA-Interim_historical",
        # directory is not constant across all groups
        "directory": "/archive/DYNDOWN/DIONE/pbieniek/erain/hourly",
    },
    "gfdl_hist": {
        "years": np.arange(1970, 2007),
        "fn_str": "GFDL-CM3_historical",
        "directory": "/archive/DYNDOWN/DIONE/pbieniek/gfdl/hist/hourly",
    },
    "ccsm_hist": {
        "years": np.arange(1970, 2006),
        "fn_str": "NCAR-CCSM4_historical",
        "directory": "/archive/DYNDOWN/DIONE/pbieniek/ccsm/hist/hourly",
    },
    "gfdl_rcp85": {
        "years": np.arange(2006, 2101),
        "fn_str": "GFDL-CM3_rcp85",
        # this is the oddball one
        "directory": "/archive/DYNDOWN/DIONE/rtladerjr/hourly",
    },
    "ccsm_rcp85": {
        "years": np.arange(2005, 2101),
        "fn_str": "NCAR-CCSM4_rcp85",
        "directory": "/archive/DYNDOWN/DIONE/pbieniek/ccsm/rcp85/hourly",
    },
}

group_fn_lu = {
    groups[key]["fn_str"]: key for key in groups
}


wind_varnames = ["U", "U10", "UBOT", "V", "V10", "VBOT"]

accum_varnames = ["ACSNOW", "PCPT", "PCPC", "PCPNC", "POTEVP"]

# names of variables that should be resampled to daily
resample_varnames = ["T2", "T2MIN", "T2MAX", "Q2", "PCPC", "PCPT"]

lon_variable = "g5_lon_1"
lat_variable = "g5_lat_0"

# new names for "level" variables
levelnames = {
    "lv_ISBL2": "plev",
    "lv_DBLY3": "depth",
}
rev_levelnames = {
    value: key for key, value in zip(levelnames.keys(), levelnames.values())
} 

coord_attrs = {
    "xc": {
        "standard_name": "projection_x_coordinate",
        "units": "m",
    },
    "yc": {
        "standard_name": "projection_y_coordinate",
        "units": "m",
    },
    "time": {
        "time zone": "UTC"
    },
    # pulled manually from some CMIP6 lon/lat attrs
    "lon": {
        "standard_name": "longitude",
        "title": "Longitude",
        "units": "degrees_east",
        "valid_max": 180.0,
        "valid_min": -180.0,
    },
    "lat": {
        "standard_name": "latitude",
        "title": "Latitude",
        "units": "degrees_north",
        "valid_max": 90.0,
        "valid_min": -90.0,
    }
}

global_attrs = {
    # proj4 string derived from wrf-python library in the
    #  ancillary/include_latlon.ipynb notebook
    "proj_parameters": (
        "+proj=stere +units=m +a=6370000.0 +b=6370000.0 +lat_0=90.0 "
        "+lon_0=-152.0 +lat_ts=64.0"
    ),
    "restacked_by": "Scenarios Network for Alaska + Arctic Planning -- 2022",
    "version": "1.1",
}


var_attrs = {
    "PSFC": {"long_name": "Surface pressure", "units": "Pa"},
    "PBOT": {"long_name": "Pressure", "units": "Pa"},
    "GHT": {"long_name": "Geopotential height", "units": "gpm"},
    "ZBOT": {"long_name": "Geopotential height", "units": "gpm"},
    "TSK": {"long_name": "Surface skin temperature", "units": "K"},
    "T": {"long_name": "Temperature", "units": "K"},
    "T2": {"long_name": "Temperature at 2m height", "units": "K"},
    "TBOT": {"long_name": "Temperature at lowest model level", "units": "K"},
    "U": {"long_name": "u-component of wind", "units": "m/s"},
    "U10": {"long_name": "u-component of wind at 10m height", "units": "m/s"},
    "UBOT": {"long_name": "u-component of wind at lowest model level", "units": "m/s"},
    "V": {"long_name": "v-component of wind", "units": "m/s"},
    "V10": {"long_name": "v-component of wind at 10m height", "units": "m/s"},
    "VBOT": {"long_name": "v-component of wind at lowest model level", "units": "m/s"},
    "OMEGA": {"long_name": "Pressure vertical velocity", "units": "Pa/s"},
    "QVAPOR": {"long_name": "Specific humidity", "units": "kg/kg"},
    "Q2": {"long_name": "Specific humidity at 2m height", "units": "kg/kg"},
    "QBOT": {"long_name": "Specific humidity at lowest model level", "units": "kg/kg"},
    "PCPT": {"long_name": "Total precipitation", "units": "mm"},
    "PCPNC": {"long_name": "Accumulated large-scale precipitation", "units": "kg/m^2"},
    "PCPC": {"long_name": "Accumulated convective precipitation", "units": "kg/m^2"},
    "SNOW": {"long_name": "Snow water equivalent", "units": "kg/m^2"},
    "ACSNOW": {"long_name": "Water equivalent of accum. snow depth", "units": "kg/m^2"},
    "SNOWH": {"long_name": "Snow depth", "units": "m"},
    "CLDFRA": {"long_name": "Total cloud cover", "units": "%"},
    "CLDFRA_LOW": {"long_name": "Low level cloud cover", "units": "%"},
    "CLDFRA_MID": {"long_name": "Mid level cloud cover", "units": "%"},
    "CLDFRA_HIGH": {"long_name": "High level cloud cover", "units": "%"},
    "ALBEDO": {"long_name": "Albedo", "units": "%"},
    "TSLB": {"long_name": "Soil temperature", "units": "K"},
    "VEGFRA": {"long_name": "Vegetation fraction", "units": "%"},
    "SEAICE": {"long_name": "Ice concentration (ice=1;no ice=0)", "units": "fraction"},
    "LH": {"long_name": "Latent heat flux", "units": "W/m^2"},
    "HFX": {"long_name": "Sensible heat flux", "units": "W/m^2"},
    "SLP": {"long_name": "Mean sea level pressure (ETA model)", "units": "Pa"},
    "SMOIS": {"long_name": "Volumetric soil moisture content", "units": "fraction"},
    "SWUPBC": {"long_name": "Clear sky upward solar flux", "units": "W/m^2"},
    "SWDNBC": {"long_name": "Clear sky downward solar flux", "units": "W/m^2"},
    "LWUPBC": {"long_name": "Clear sky upward long wave flux", "units": "W/m^2"},
    "LWDNBC": {"long_name": "Clear sky downward long wave flux", "units": "W/m^2"},
    "SWDNB": {"long_name": "Downward short wave flux", "units": "W/m^2"},
    "LWDNB": {"long_name": "Downward long wave flux", "units": "W/m^2"},
    "SWUPB": {"long_name": "Upward short wave flux", "units": "W/m^2"},
    "LWUPB": {"long_name": "Upward long wave flux", "units": "W/m^2"},
    "CANWAT": {"long_name": "Plant canopy surface water", "units": "kg/m^2"},
    "POTEVP": {"long_name": "Accumulated potential evaporation", "units": "kg/m^2"},
    "SNOWC": {"long_name": "Snow cover", "units": "fraction"},
    "SH2O": {
        "long_name": "Liquid volumetric soil moisture (non-frozen)",
        "units": "fraction",
    },
    "lv_DBLY3": {
        "long_name": "layer between two depths below land surface",
        "units": "cm",
    },
    "lv_DBLY3_l1": {
        "long_name": "layer between two depths below land surface",
        "units": "cm",
    },
    "lv_DBLY3_l0": {
        "long_name": "layer between two depths below land surface",
        "units": "cm",
    },
    "lv_ISBL2": {"long_name": "isobaric level", "units": "hPa"},
    "g5_rot_2": {"long_name": "vector rotation angle", "units": "radians"},
    "g5_lat_0": {"long_name": "latitude", "units": "degrees_north"},
    "g5_lon_1": {"long_name": "longitude", "units": "degrees_east"},
}

# all variable names
# these are all the variables that can actually be restacked
varnames = [
    "ACSNOW",
    "T2",
    "CANWAT",
    "CLDFRA",
    "HFX",
    "LH",
    "LWDNB",
    "LWUPB",
    "PCPC",
    "PCPNC",
    "PCPT",
    "POTEVP",
    "QBOT",
    "Q2",
    "SNOW",
    "QVAPOR",
    "SNOWC",
    "SNOWH",
    "SWUPB",
    "SWDNB",
    "TSLB",
    "ALBEDO",
    "VEGFRA",
    "CLDFRA_HIGH",
    "CLDFRA_LOW",
    "CLDFRA_MID",
    "LWUPBC",
    "LWDNBC",
    "GHT",
    "OMEGA",
    "PSFC",
    "SLP",
    "SH2O",
    "SEAICE",
    "SWUPBC",
    "SMOIS",
    "SWDNBC",
    "TBOT",
    "TSK",
    "T",
    "U",
    "V",
    "U10",
    "V10",
    "UBOT",
    "VBOT",
]

# for resampling to daily, names of target variable names
#  and associated WRF variable names
resample_varnames = {
    "T2MAX": {
        "wrf_varname": "t2",
        "aggr": "max",
    },
    "T2MIN": {
        "wrf_varname": "t2",
        "aggr": "min",
    },
    "T2": {
        "wrf_varname": "t2",
        "aggr": "mean",
    },
    "Q2": {
        "wrf_varname": "q2",
        "aggr": "mean",
    },
    "PCPC": {
        "wrf_varname": "pcpc",
        "aggr": "mean",
    },
    "PCPT": {
        "wrf_varname": "pcpt",
        "aggr": "sum",
    },
}
