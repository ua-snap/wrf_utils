# zip all Monthly files in a single zip -- for PKafle since they are small-ish

def list_files( base_path, ext='.nc' ):
	return [ os.path.join(r,fn) for r,s,files in os.walk( base_path ) 
				for fn in files if fn.endswith(ext) ]

if __name__ == '__main__':
	import os, zipfile, zlib
	import pandas as pd
	import numpy as np

	base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/monthly'
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/data_zips'

	files = list_files( base_path, ext='.nc' )	
	out_fn = os.path.join( output_path, 'wrf_monthly_allgroups.zip' )

	# open a new zipfile
	zf = zipfile.ZipFile( out_fn, mode='w' )
		
	for fn in files:
		zf.write( fn, arcname=os.path.basename(fn), compress_type=zlib.DEFLATED )
	
	# cleanup
	zf.close()
	zf = None
	del zf
