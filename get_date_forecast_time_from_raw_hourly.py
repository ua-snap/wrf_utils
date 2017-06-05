# get attrs from dset
# from peter we need to find out what times are missing...

# # CODE FROM PETERS NC Generate Run: in relation to that time var.
# if (ftime == 6) THEN
#    call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','yes') )
# else   
#    call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','no') )
# endif

# varname = forecast_time

def get_month_day( fn ):
	dirname, basename = os.path.split( fn )
	year, month, day_hour = basename.split('.')[-2].split('-')
	day, hour = day_hour.split( '_' )
	folder_year = dirname.split('/')[-1]
	return {'fn':fn, 'year':year, 'folder_year':folder_year,'month':month, 'day':day, 'hour':hour}
def get_forecast_time( fn ):
	return xr.open_dataset( fn, decode_times=False )[ 'PCPT' ].attrs[ 'forecast_time' ]
def get_file_attrs( fn ):
	fn_args = get_month_day( fn )
	fn_args.update( forecast_time=get_forecast_time( fn ) )
	return fn_args

if __name__ == '__main__':
	import xarray as xr
	import pandas as pd
	import multiprocessing as mp
	import os
	
	# fn = '/storage01/pbieniek/gfdl/hist/hourly/1970/WRFDS_d01.1970-01-02_01.nc'
	base_path = '/storage01/pbieniek/gfdl/hist/hourly'
	fn_list = [ os.path.join(r, fn) for r,s,files in os.walk(base_path) if 'oldstuff' not in r for fn in files if fn.endswith('.nc') ]
	ncpus = 10
	
	pool = mp.Pool( ncpus )
	out = pool.map( get_file_attrs, fn_list )
	pool.join()
	pool.close()

	df = pd.DataFrame( out )



