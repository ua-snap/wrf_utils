def list_files( path ):
	return glob.glob(os.path.join(path, '*.nc'))

def test_profile( fn ):
	try:
		variable = os.path.basename(fn).split('_')[0]
		with xr.open_dataset(fn) as ds:
			arr = ds[variable].load()
		if len(arr.shape) == 3:
			profile = arr[:,100,100]
		elif len(arr.shape) == 4:
			profile = arr[:,:,100,100]
		else:
			raise BaseException('wrong number of dimensons')
		if np.isnan(profile).all():
			return 'bad file'
		else:
			return 'good file'
	except:
		return 'bad file'

def run_test( path ):
	files = list_files( path )
	variable = path.split('/')[-1]
	# return {variable:{ fn:test_profile(fn) for fn in files }}
	return { fn:test_profile(fn) for fn in files }


if __name__ == '__main__':
	import os, rasterio
	import numpy as np
	import pandas as pd
	import os, glob
	import xarray as xr
	import multiprocessing as mp

	rcs_path = '/rcs/project_data/wrf_data/hourly_fix'
	variables = ['acsnow','albedo','canwat','cldfra',
				'ght','hfx','lh','lwdnb','lwdnbc','lwupb','lwupbc','omega','pcpc','pcpnc',
				'pcpt','potevp','psfc','q2','qbot','qvapor','seaice','sh2o','slp','smois','snow',
				'snowc','snowh','swdnb','swdnbc','swupb','swupbc','t','t2','tbot','tsk','tslb',
				'u','u10','ubot','v','v10','vbot','vegfra',]

				# CHECK THESE: 'cldfra_high','cldfra_low','cldfra_mid',
	paths = [os.path.join(rcs_path, variable) for variable in variables]

	pool = mp.Pool(10)
	out = pool.map(run_test, paths)
	pool.close()
	pool.join()

