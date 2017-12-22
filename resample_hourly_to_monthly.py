# resample hourly to monthly for data delivery
import xarray as xr
import numpy as np
import pandas as pd
import os, glob, itertools

base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf'
wildcard = '*.nc'
# variable = 'T2' # 'PCPT', 
variables = [ os.path.basename(i).upper() for i in glob.glob( os.path.join(base_path, 'hourly', '*' ) ) if os.path.isdir( i ) and 'slurm' not in i ]

for variable in variables:
    files = sorted( glob.glob( os.path.join( base_path, 'hourly', variable.lower(), wildcard ) ) )
    for fn in files:
        print( 'working on: {} - {}'.format( fn, variable ) )
        ds = xr.open_mfdataset( fn, autoclose=True )

        # get some attrs
        global_attrs = ds.attrs
        local_attrs = ds[ variable ].attrs
        xy_attrs = ds.lon.attrs
        time_attrs = ds.time.attrs

        # pathing
        # input_path = os.path.join( base_path, 'hourly', variable )
        output_path = os.path.join( base_path, 'hourly_to_monthly', variable.lower() )

        if not os.path.exists( output_path ):
            os.makedirs( output_path )

        # metric switch -- aggregation
        if variable is 'PCPT' or 'pcpt':
            metric = 'sum'
        else:
            metric = 'mean'

        ds_mon = ds.resample( 'M', dim='time', how=metric )
        ds_mon_comp = ds_mon.compute() # watch this one

        # ds_mon_comp = ds_mon_comp.to_dataset( name=variable )
        ds_mon_comp.attrs = global_attrs
        ds_mon_comp[ variable ].attrs = local_attrs
        ds_mon_comp[ 'lon' ].attrs = xy_attrs
        ds_mon_comp[ 'lat' ].attrs = xy_attrs
        ds_mon_comp[ 'time' ].attrs = time_attrs

        # dump to disk
        dirname, basename = os.path.split( fn )
        basename = basename.replace( 'hour', 'month' ).replace( '.nc', '_{}.nc'.format(metric) )
        output_filename = os.path.join( output_path, basename )

        # set output compression and encoding for serialization
        encoding = ds_mon_comp[ variable ].encoding
        encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
        ds_mon_comp[ variable ].encoding = encoding

        ds_mon_comp.to_netcdf( output_filename, mode='w', format='NETCDF4_CLASSIC' )

        # cleanup file handles
        ds.close()
        ds = None
        ds_mon.close()
        ds_mon = None
        ds_mon_comp.close()
        ds_mon_comp = None