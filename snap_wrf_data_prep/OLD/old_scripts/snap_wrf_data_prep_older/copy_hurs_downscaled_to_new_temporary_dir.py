# copy the new hurs data to a common temporary dir
def copy_file( fn, out_fn ):
	dirname = os.path.dirname( out_fn )
	try:
		if not os.path.exists( dirname ):
			_ = os.makedirs( dirname )
	except:
		pass
	return shutil.copy( fn, out_fn )

def wrap( x ):
	copy_file( *x )

if __name__ == '__main__':
	import os, shutil
	import multiprocessing as mp

	path = '/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/downscaled'
	out_path = '/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/downscaled_hurs_temporary'

	files = [ os.path.join( r, fn ) for r,s,files in os.walk( path ) for fn in files if fn.endswith('.tif') and 'hurs_' in fn and 'anom' not in fn ]
	out_files = [ fn.replace('/downscaled/', '/downscaled_hurs_temporary/') for fn in files ]

	pool = mp.Pool( 32 )
	out = pool.map( wrap, zip(files, out_files) )
	pool.close()
	pool.join()
