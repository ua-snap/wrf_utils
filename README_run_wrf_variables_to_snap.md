### How I am Running Variables
---
##### PROCESSING STEPS (follow the order, one-at-a-time):

1. run stacking procedure for all 3 groups (`erain`,`historical`,`rcp85`)
2. run `resample_hourly_to_daily.py`
	- output daily files from hourly originals
3. run `improve_snap_stacked_outputs_daily.py`
	- updates metadata and reassigns coords to the files for ease of use.
4. run `resample_daily_to_monthly.py`
	- resample _all_ dailies to monthlies
5. run `improve_snap_stacked_outputs_hourly.py`
	- updates metadata and reassigns coords to the HOURLY files for ease of use.
6. copy/move the files into the existing `wrf_data` folder of all completed outputs.
	- finalizes variables for current needs