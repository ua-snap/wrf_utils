# TEST THE COORDINATE ISSUE FOR MYLENE

import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot as plt
import xarray as xr
import numpy as np
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd

fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/elevation_msl_wrf_alaska.nc'
elev = xr.open_dataset( fn )

fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/t2/t2_hourly_wrf_ERA-Interim_historical_1979.nc'
ds = xr.open_dataset(fn)

elev.elevation.plot()
plt.savefig( '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/elev.png' )
plt.close()

ds.t2.sel(time='1979-12-31').isel(time=0).plot()
plt.savefig( '/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/t2.png' )
plt.close()


# pull the lat/longs and reproject them.
lats = elev.lat
lons = elev.lon

lonlat = np.array([lons.data.ravel(),lats.data.ravel()]).T.tolist()
pts = [Point(*i) for count, i in enumerate(lonlat)]
ids = list(range(len(pts)))
data = list(zip(ids, pts))
arr = ds.t2.isel( time=0 ).data.ravel()
# pts = [ dict(count,Point(*i)) for count, i in enumerate(lonlat) ]

df = gpd.GeoDataFrame( data, columns=['ids','geometry'], crs={'init':'epsg:4326'} )
df['t2'] = arr
df.to_file('/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/test_points.shp')
df2 = df.to_crs(ds.proj_parameters)
df2.to_file('/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/test_points_2.shp')


# pull the xc/yc
yc = elev.yc
xc = elev.xc
xc,yc = np.meshgrid(xc.data,yc.data)

xcyc = np.array([xc.ravel(),yc.ravel()]).T.tolist()
pts = [Point(*i) for count, i in enumerate(xcyc)]
ids = list(range(len(pts)))
data = list(zip(ids, pts))
arr = ds.t2.isel( time=0 ).data.ravel()

df = gpd.GeoDataFrame( data, columns=['ids','geometry'], crs=ds.proj_parameters )
df['t2'] = arr
df.to_file('/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/test_points_xcyc.shp')

df2 = df.to_crs(epsg=4326)
df2.to_file('/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/testing/test_points_xcyc_4326.shp')


