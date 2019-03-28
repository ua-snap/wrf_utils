# make an ancillary file that has some of the common mdata from the other WRF files

import xarray as xr
import numpy as np

wrf_template_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_hourly_wrf_GFDL-CM3_historical_1974.nc'

tmp_ds = xr.open_dataset(wrf_template_fn)

ancillary_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc'

ds = xr.open_dataset(ancillary_fn)

# get the attrs and the coords
file_attrs = tmp_ds.attrs 
var_attrs = ds.HGT_M.attrs
elevation = ds.HGT_M.values

xc = tmp_ds.xc
yc = tmp_ds.yc
lat = tmp_ds.lat
lon = tmp_ds.lon

new_ds = xr.Dataset({'elevation': (['yc', 'xc'], np.flipud(elevation.squeeze()))},
                    coords={'xc': xc,
                            'yc': yc,
                            'lon':lon,
                            'lat':lat
                            }, attrs=file_attrs)

new_ds['elevation'].attrs = var_attrs


new_ds.to_netcdf('/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/elevation_msl_wrf_alaska.nc')
