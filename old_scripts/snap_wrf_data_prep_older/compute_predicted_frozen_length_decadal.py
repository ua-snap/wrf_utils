# compute new length of frozen season (predicted). The idea here is to compute the decadal average LOFS and remove the LTA from that
# then subtract the inital LOFS from the diff and voila! a new variable is born. :)

if __name__ == '__main__':
	import os, rasterio, glob
	import xarray as xr
	import numpy as np

	scenarios = ['rcp45','rcp85']
	years = list(range(2020,2091, 10))
	base_dir = '/workspace/Shared/Tech_Projects/DOD_Ft_Wainwright/project_data/GIPL/SNAP_modified/frozen_season_length'

	for scenario in scenarios:
		# frozen season length
		lofs_fn = os.path.join(base_dir, 'decadal','gipl2f_frozen_length_5cm_ar5_5modelAvg_{}_1km_ak_Interior_decadal_2020-2090.nc'.format(scenario))

		lofs = xr.open_dataset( lofs_fn )

		# frozen season length diff from baseline
		lofs_diff_fn = os.path.join( base_dir, 'decadal_diff_from_baseline','freeze_to_thaw_length_decadal_diff_{}_cru_lta_2020-2090.tif'.format(scenario) )
		
		with rasterio.open( lofs_diff_fn ) as rst:
			lofs_diff = rst.read()
			meta = rst.meta.copy()
			meta.update( count=1, compress='lzw' )

		# make a new season length raster
		new_season_length = lofs.frozen_length - lofs_diff
		new_season_length.data[lofs_diff == -9999] = -9999

		for count,year in enumerate(years):
			output_filename = os.path.join( base_dir, 'decadal_predicted_season_length', 'gipl_predicted_decadal_avg_frozen_length_{}_{}.tif'.format(scenario, str(year)) )
			with rasterio.open( output_filename, 'w', **meta ) as out:
				out.write( new_season_length[count].values.astype( np.float32 ), 1 )