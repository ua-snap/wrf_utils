def get_mod_times( path ):
	a = {os.path.basename(fn):os.path.getmtime(fn) for fn in glob.glob(os.path.join(path, '*'))}
	dt = pd.Series({i:datetime.datetime.fromtimestamp(time.mktime(time.gmtime(a[i]))) for i in a }).max()
	return dt

if __name__ == '__main__':
	import os, rasterio
	import numpy as np
	import pandas as pd
	import os, glob

	rcs_path = '/rcs/project_data/wrf_data/hourly_fix'
	shared_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix'
	storage01_path = '/storage01/malindgren/wrf_ccsm4/hourly_fix'

	variables = ['acsnow','albedo','canwat','cldfra','cldfra_high','cldfra_low','cldfra_mid','ght','hfx','lh','lwdnb','lwdnbc','lwupb','lwupbc','omega','pcpc','pcpnc','pcpt','potevp','psfc','q2','qbot','qvapor','seaice','sh2o','slp','smois','snow','snowc','snowh','swdnb','swdnbc','swupb','swupbc','t','t2','tbot','tsk','tslb','u','u10','ubot','v','v10','vbot','vegfra',]

	out = {}
	for variable in variables:
		curvar = {'rcs' : get_mod_times(os.path.join(rcs_path,'t2')),
				'shared' : get_mod_times(os.path.join(shared_path,'t2')),
				'storage01' : get_mod_times(os.path.join(storage01_path,'t2'))}
		out[variable] = curvar

		
	# find newest:
	maxones = {} 
	for variable in variables:
		curvar = out[variable]
		maxval = max(curvar.values())
		curmax = {i:curvar[i] for i in curvar if curvar[i] == maxval}
		maxones[variable] = curmax
