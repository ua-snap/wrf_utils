# list and count all WRF files for completion assessment
import os
import pandas as pd
import numpy as np

# base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'
base_dir = '/storage01/malindgren/wrf_ccsm4/hourly'
files = [ os.path.join(r,fn) for r,s,files in os.walk( base_dir ) for fn in files if fn.endswith('.nc') ]
colnames = ['variable','output', 'timestep', 'model', 'scenario', 'year']
files_elems = [ os.path.splitext(os.path.basename(fn))[0].split('_') for fn in files ]
files_elems_out = []
for i in files_elems: # DEAL WITH THE CLDFRA_MID, etc.
	if len(i) == 7:
		new_val = '-'.join([i.pop(0) for idx in [0,1]])
		i = [new_val] + i
	else:
		i
	files_elems_out = files_elems_out + [i]

df = pd.DataFrame( files_elems_out, columns=colnames )
counts = list(zip(*[ i.ravel().tolist() for i in np.unique(df.variable, return_counts=True)]))
