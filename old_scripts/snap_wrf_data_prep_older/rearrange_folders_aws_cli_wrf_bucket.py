# rearrange the files in S3
def move_em( args ):
   return subprocess.call( args )

if __name__ == '__main__':
   import subprocess, os
   import multiprocessing as mp

   variables = ['acsnow','albedo','canwat','cldfra','cldfra_high','cldfra_low','cldfra_mid','ght','hfx','lh','lwdnb','lwdnbc','lwupb','lwupbc','omega','pcpc','pcpnc','pcpt','potevp','psfc','q2','qbot','qvapor','seaice','sh2o','slp','smois','snow','snowc','snowh','swdnb','swdnbc','swupb','swupbc','t','t2','tbot','tsk','tslb','vegfra']

   models = [ 'ERA-Interim', 'GFDL-CM3', 'NCAR-CCSM4' ]

   base_path = 's3://wrf-ak-ar5/'

   args = []
   for model in models:
      for variable in variables:
         cur_path = base_path+'hourly/'+variable
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
