#!/bin/bash

folders='tslb qvapor ght t'
indir='/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly/'
# indir='/storage01/malindgren/wrf_ccsm4/hourly_fix/'
outdir='/rcs/project_data/wrf_data/hourly/'

for folder in $folders
	do
		rsync -rvhu --progress $indir$folder $outdir
	done

