# move completed wrf files to 'done' folder:

def copy_file( fn, out_fn ):
	dirname, basename = os.path.split(out_fn)
	print( basename )
	try:
		if not os.path.exists( dirname ):
			os.makedirs( dirname )
	except:
		pass

	return shutil.copy2( fn, out_fn )

def wrap( x ):
	return copy_file( *x )

if __name__ == '__main__':
	import os, shutil
	import multiprocessing as mp

	base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
	output_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'

	# variables we want to move...
	completed_vars ['potevp', 'canwat', 'tbot', 'tsk', 'seaice']
	variables = [ i for i in os.listdir(base_dir) if i not in completed_vars ]

	args = [ (os.path.join(r,fn),os.path.join(r,fn).replace(base_dir, output_dir)) 
				for r,s,files in os.walk(base_dir) 
					for fn in files 
						if fn.endswith('.nc') and any(variable.upper() in fn for variable in variables ) ]

	pool = mp.Pool( 64 )
	out = pool.map( wrap, args )
	pool.close()
	pool.join()