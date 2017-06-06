# get forecast_time attrs from dset to use in interpolation

def get_month_day( fn ):
	dirname, basename = os.path.split( fn )
	year, month, day_hour = basename.split('.')[-2].split('-')
	day, hour = day_hour.split( '_' )
	folder_year = dirname.split('/')[-1]
	return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}
def get_forecast_time( fn ):
	ds = xr.open_dataset( fn, decode_times=False, autoclose=True )
	forecast_time = ds[ 'PCPT' ].attrs[ 'forecast_time' ]
	ds.close() # keep it clean with lotsa i/o
	return forecast_time
def get_file_attrs( fn ):
	try:
		fn_args = get_month_day( fn )
		fn_args.update( forecast_time=get_forecast_time( fn ) )
	except:
		# if there is an issue... dont fail, do this...
		fn_args = {'fn':-9999, 'year':-9999, 'folder_year':-9999,'month':-9999, 'day':-9999, 'hour':-9999, 'forecast_time':-9999}
	return fn_args

if __name__ == '__main__':
	import xarray as xr
	import pandas as pd
	import multiprocessing as mp
	import os
	
	# setup args
	# base_path = '/storage01/pbieniek/gfdl/hist/hourly'
	base_path = '/storage01/pbieniek/erain/hourly'
	# base_path = '/storage01/rtladerjr/hourly'
	fn_list = [ os.path.join(r, fn) for r,s,files in os.walk(base_path) if 'oldstuff' not in r for fn in files if fn.endswith('.nc') and 'test' not in fn ]
	ncpus = 32
	output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs'
	# group = 'gfdl_hist'
	group = 'erain'
	# group = 'gfdl_rcp85'

	pool = mp.Pool( ncpus )
	out = pool.map( get_file_attrs, fn_list )
	pool.close()
	pool.join()

	df = pd.DataFrame( out )
	output_filename = os.path.join( output_path, 'WRFDS_forecast_time_attr_{}.csv'.format( group ) )
	df.to_csv( output_filename, sep=',' )

# # NOTES:
# # # CODE FROM PETERS NC Generate Run: in relation to that time var.
# if (ftime == 6) THEN
#    call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','yes') )
# else   
#    call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','no') )
# endif

# varname = forecast_time
