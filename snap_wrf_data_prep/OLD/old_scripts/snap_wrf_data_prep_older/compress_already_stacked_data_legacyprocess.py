# # # # # # # # # # # # # # # # # # # # # # # # # # # 
# COMPRESS THE ALREADY GENERATED DATA ... NOW BUILT INTO THE STACKING AND MONTHLY AGGREGATIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # 

def re_encode( x, variable ):
	import xarray as xr
	import numpy as np

	in_fn, out_fn = x

	ds = xr.open_dataset( in_fn, autoclose=True )

	encoding = ds[ variable ].encoding
	# chunksize = ds[ variable ].shape
	encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
	
	ds[ variable ].encoding = encoding
	ds.to_netcdf( out_fn )

	# clean up (forcibly)
	ds.close()
	ds = None
	del ds
	
	return out_fn


if __name__ == '__main__':
	import os, glob
	import xarray as xr
	import numpy as np
	from functools import partial
	import multiprocessing as mp

	base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_compress/hourly'

	variables = [ i for i in os.listdir( base_path ) if 'slurm' not in i ]

	for variable in variables:
		print( variable )
		if variable.lower() == 'qvapor': # since we already ran it...

			if not os.path.exists( os.path.join( output_path, variable ) ):
				os.makedirs( os.path.join( output_path, variable ) )

			l = glob.glob( os.path.join( base_path, variable, '*.nc' ) )
			output_filenames = [ i.replace( base_path, output_path ) for i in l ]

			f = partial( re_encode, variable=variable.upper() )

			pool = mp.Pool( 2 )
			done = pool.map( f, list( zip( l, output_filenames ) ) )
			pool.close()
			pool.join()

			# cleanup -- not necessary (probably), but smart
			pool = None
			del pool
