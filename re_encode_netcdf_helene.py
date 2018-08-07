# # # # # # # # # # # # # # # # # # # # # # # # # # # 
# COMPRESS THE ALREADY GENERATED DATA ... NOW BUILT INTO THE STACKING AND MONTHLY AGGREGATIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # 

# # # THIS WAS USED IN THE RE-ENCODING FOR HELENE's DATA x-fer
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

	base_path = '/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/helene_zipped_raw_prepped/test'
	output_path = '/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/helene_zipped_raw_prepped/test_compress'

	if not os.path.exists( output_path ):
		os.makedirs( output_path )

	l = glob.glob( os.path.join( base_path, '*.nc' ) )
	output_filenames = [ i.replace( base_path, output_path ) for i in l ]

	f = partial( re_encode, variable='hur' )

	pool = mp.Pool( 5 )
	done = pool.map( f, list( zip( l, output_filenames ) ) )
	pool.close()
	pool.join()

	# cleanup -- not necessary (probably), but smart
	pool = None
	del pool
