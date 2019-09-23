def run( fn, command, slurm_path, ncpus=32 ):
    import os, subprocess
    head = '#!/bin/sh\n' + \
            '#SBATCH --ntasks={}\n'.format(ncpus) + \
            '#SBATCH --nodes=1\n' + \
            '#SBATCH --ntasks-per-node={}\n'.format(ncpus) + \
            '#SBATCH --account=snap\n' + \
            '#SBATCH --mail-type=FAIL\n' + \
            '#SBATCH --mail-user=malindgren@alaska.edu\n' + \
            '#SBATCH -p viz,main\n'
    
    with open( fn, 'w' ) as f:
        f.write( head + '\n' + command + '\n' )
    
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import subprocess, os

    slurm_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/aws_move_slurm'

    variables = ['tsk', 't2', 'albedo', 'acsnow', 'canwat', 'cldfra', 'cldfra_high', 'cldfra_low', \
    'cldfra_mid', 'lwupb', 'lwdnbc', 'lwdnb', 'lh', 'hfx', 'ght',\
    'pcpc', 'potevp', 'psfc', 'lwupbc', 't', 'v', 'pcpnc', 'omega', 'seaice', 'slp', 'smois',\
    'sh2o', 'snowc', 'tbot', 'snowh', 'snow', 'qbot', 'qvapor', 'swupbc', 'swdnbc',\
    'swdnb', 'swupb', 'vegfra', 'ubot', 'u10', 'u', \
    'tslb', 'v10', 'vbot', 'q2',]
    
    for variable in variables:
        input_dir = os.path.join('/rcs/project_data/wrf_data/hourly_fix',variable)
        models = ['NCAR-CCSM4_historical', 'NCAR-CCSM4_rcp85', 'GFDL-CM3_historical', 'GFDL-CM3_rcp85', 'ERA-Interim_historical']

        for model in models:        
            modname, scenario = model.split('_')
            output_remote_dir = 's3://wrf-ak-ar5/hourly/{}/{}/{}'.format( modname, scenario, variable )
            command = ' '.join(['aws','s3','cp',input_dir,output_remote_dir,'--recursive','--exclude', '"*"', ' --include', '"*{}*.nc"'.format(model)])
            fn = os.path.join( slurm_path, '{}_{}_move_s3_aws.slurm'.format(variable, model) )
            run( fn, command, slurm_path, ncpus=10 )
