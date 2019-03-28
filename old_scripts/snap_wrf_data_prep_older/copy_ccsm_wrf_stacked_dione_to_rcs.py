# move all wrf NCAR-CCSM4 hourlies to the /rcs drive same as the GFDL...

def copy_data( in_fn, out_fn ):
	if not os.path.exists(out_fn):
		print(in_fn)
		return subprocess.call(['cp',in_fn,out_fn])
	else:
		if os.stat(out_fn).st_size != os.stat(in_fn).st_size:
			print(in_fn)
			return subprocess.call(['cp',in_fn,out_fn])

def wrap(x):
	return copy_data(*x)

if __name__ == '__main__':
	import os, subprocess
	import multiprocessing as mp

	ncpus = 6
	dione_path = '/storage01/malindgren/wrf_ccsm4/hourly'
	rcs_path = '/rcs/project_data/wrf_data/hourly'

	dione_folders = [os.path.join(dione_path, i) for i in os.listdir(dione_path)]

	dione_files = [os.path.join(r,fn) for r,s,files in os.walk(dione_path) for fn in files if fn.endswith('.nc')]
	out_files = [i.replace(dione_path, rcs_path) for i in dione_files]

	args = list(zip(dione_files, out_files))

	pool = mp.Pool(ncpus)
	out = pool.map(wrap, args)
	pool.close()
	pool.join()


# # ERRORS: (RE-RUN THESE)
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1993.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/t/T_wrf_hourly_ccsm_rcp85_2091.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2060.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1989.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2052.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/t/T_wrf_hourly_ccsm_rcp85_2080.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2053.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1983.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/ght/GHT_wrf_hourly_ccsm_rcp85_2074.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2017.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2023.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2063.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2073.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2075.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2076.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2083.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2084.nc': Input/output error
# cp: closing `/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2085.nc': Input/output error


# FIX FAILED FILES -- THIS WAS HACKILY PUT TOGETHER THROUGH THE ERROR MESSAGES AND
#  WE ARE RE-TRYING THEM
def copy_data( in_fn, out_fn ):
	print(in_fn)
	if os.path.exists(out_fn):
		_ = os.unlink(out_fn)
	return subprocess.call(['cp',in_fn,out_fn])

def wrap(x):
	return copy_data(*x)

if __name__ == '__main__':
	import os, subprocess

	dione_path = '/storage01/malindgren/wrf_ccsm4/hourly'
	rcs_path = '/rcs/project_data/wrf_data/hourly'

# 	out_files = ['/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1993.nc',
# 		'/rcs/project_data/wrf_data/hourly/t/T_wrf_hourly_ccsm_rcp85_2091.nc',
# 		'/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2060.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1989.nc',
# 		'/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2052.nc',
# 		'/rcs/project_data/wrf_data/hourly/t/T_wrf_hourly_ccsm_rcp85_2080.nc',
# 		'/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2053.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1983.nc',
# 		'/rcs/project_data/wrf_data/hourly/ght/GHT_wrf_hourly_ccsm_rcp85_2074.nc',
# 		'/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2017.nc',
# 		'/rcs/project_data/wrf_data/hourly/qvapor/QVAPOR_wrf_hourly_ccsm_rcp85_2023.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2063.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2073.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2075.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2076.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2083.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2084.nc',
# 		'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2085.nc',]

	out_files = ['/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_hist_1989.nc',
			'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2083.nc',
			'/rcs/project_data/wrf_data/hourly/omega/OMEGA_wrf_hourly_ccsm_rcp85_2084.nc',]

	in_files = [fn.replace(rcs_path,dione_path) for fn in out_files]

	args = list(zip(in_files, out_files))

	final_try = [ wrap(x) for x in args ]
