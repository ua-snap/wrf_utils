# rearrange the files in S3
def move_em( args ):
   return subprocess.call( args )

if __name__ == '__main__':
   import subprocess, os
   import multiprocessing as mp

   variables = ['pcpt','t','t2','t2max','t2min']
   models = [ 'ERA-Interim', 'GFDL-CM3', 'NCAR-CCSM4' ]
   base_path = 's3://wrf-ak-ar5/'

   args = []
   for model in models:
      for variable in variables:
         cur_path = base_path+'daily/'+variable
         if model != 'ERA-Interim':
            scenarios = ['rcp85', 'historical']
         else:
            scenarios = ['historical']
         
         for scenario in scenarios:
            out_path = base_path+'hourly/'+model+'/'+scenario+'/'+variable
            args = args + [['aws','s3','mv', cur_path, out_path, '--recursive', '--exclude', '*','--include','*'+model+'_'+scenario+'*' ]]

   # run it in parallel
   pool = mp.Pool( 32 )
   done = pool.map( move_em, args )
   pool.close()
   pool.join()


# RENAME THE GD files:
import os
path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/daily'
files = [os.path.join(r,fn) for r,s,files in os.walk( path ) for fn in files if fn.endswith('.nc')]
out_files = []
for fn in files:
   dirname, basename = os.path.split(fn)
   new_basename = '_'.join(basename.split('_')[:-1])+'.nc'
   out_files = out_files + [os.path.join( dirname, new_basename )]

done = [ os.rename(infn, outfn) for infn, outfn in zip(files, out_files) ]

