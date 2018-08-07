# zip the hourly files in 10year chunks...

def list_files( base_path, ext='.nc' ):
	return [ os.path.join(r,fn) for r,s,files in os.walk( base_path ) 
				for fn in files if fn.endswith(ext) ]

def files_df( files ):
	varnames = [ os.path.basename(fn).split('_')[0] for fn in files ]
	years = [ fn.split('.')[0].split('_')[-1] for fn in files ]
	decades = [ year[:3]+'0s' for year in years ]
	return pd.DataFrame({'fn':files,'variable':varnames,'year':years,'decade':decades})

def zip_it( df, output_path ):
	''' zip up the decades worth of files using the prepared dataframe '''
	decade, = df.decade.unique()
	variable, = df.variable.unique()
	group_name, = df.group.unique()
	
	# make an output name for the new zipfile.
	out_fn = os.path.join( output_path, variable.lower(), '{}_hourly_wrf_{}_{}.zip'.format( variable,group_name,decade ) )

	try:
		# setup output dir
		dirname = os.path.dirname( out_fn )
		if not os.path.exists( dirname ):
			os.makedirs( dirname )
	except:
		pass

	# zip the files -- lookup how to properly do this...
	zf = zipfile.ZipFile( out_fn, mode='w' )
	for fn in df.fn.tolist():
		zf.write( fn, arcname=os.path.basename(fn), compress_type=zlib.DEFLATED )
	
	# cleanup
	zf.close()
	zf = None
	del zf
	print(out_fn)
	return out_fn


if __name__ == '__main__':
	import os, zipfile, zlib
	import pandas as pd
	import numpy as np
	import multiprocessing as mp
	from functools import partial

	base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/data_zips/welty/hourly'

	# groupnames
	groups = [ 'GFDL-CM3_historical','GFDL-CM3_rcp85','ERA-Interim' ]
	# varnames =  ['pcpc', 'pcpnc', 'pcpt', 'q2', 'snowh', 'swdnb', 't2'] # hansen
	# varnames = ['lwupb', 'q2', 'snow', 'snowh', 'swdnb', 'swupb'] # welty
	varnames = ['pcpt', 't2'] # welty hourly

	files = files_df( list_files( base_path, ext='.nc' ) )
	for group in groups:
		files.loc[files['fn'].str.contains(group), 'group'] = group

	grouped = [group_df for name, group_df in files.groupby(['group','variable','decade']) if group_df.variable.iloc[0] in varnames ]
	# done = grouped.apply( lambda x: zip_it( x, output_path=output_path ) ) # serial
	
	# multiprocess
	pool = mp.Pool( 50 )
	done = pool.map( partial( zip_it, output_path=output_path ), grouped )
	pool.close()
	pool.join()
