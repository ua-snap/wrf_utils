# zip the 3Hourly files in 10year chunks...

def list_files( base_path, ext='.nc' ):
	return [ os.path.join(r,fn) for r,s,files in os.walk( base_path ) 
				for fn in files if fn.endswith(ext) ]

def files_df( files ):
	varnames = [ os.path.basename(fn).split('_')[0] for fn in files ]
	years = [ fn.split('_')[-2] for fn in files ]
	decades = [ year[:3]+'0s' for year in years ]
	return pd.DataFrame({'fn':files,'variable':varnames,'year':years,'decade':decades})

def zip_it( df, output_path ):
	''' zip up the decades worth of files using the prepared dataframe '''
	decade, = df.decade.unique()
	variable, = df.variable.unique()
	group_name, = df.group.unique()
	
	# make an output name for the new zipfile
	out_fn = os.path.join( output_path, variable.lower(), '{}_wrf_3hourly_{}_{}.zip'.format(variable,group_name,decade) )

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
	
	return out_fn


if __name__ == '__main__':
	import os, zipfile, zlib
	import pandas as pd
	import numpy as np

	base_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/3hourly'
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/data_zips/3hourly'

	# groupnames
	groups = [ 'gfdl_hist','gfdl_rcp85','era_interim' ]

	files = files_df( list_files( base_path, ext='.nc' ) )
	for group in groups:
		files.loc[files['fn'].str.contains(group), 'group'] = group

	grouped = files.groupby(['group','variable','decade'])
	done = grouped.apply( lambda x: zip_it( x, output_path=output_path ) )
