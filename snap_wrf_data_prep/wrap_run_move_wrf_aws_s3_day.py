def run( fn, command, slurm_path, ncpus=32 ):
    import os, subprocess
    head = '#!/bin/sh\n' + \
            '#SBATCH --ntasks={}\n'.format(ncpus) + \
            '#SBATCH --nodes=1\n' + \
            '#SBATCH --ntasks-per-node={}\n'.format(ncpus) + \
            '#SBATCH --account=snap\n' + \
            '#SBATCH --mail-type=FAIL\n' + \
            '#SBATCH --mail-user=malindgren@alaska.edu\n' + \
            '#SBATCH -p main,viz\n'
    
    with open( fn, 'w' ) as f:
        f.write( head + '\n' + command + '\n' )
    
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import subprocess, os

    slurm_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/aws_move_slurm'
    variables = ['pcpc','pcpt','q2','t2','t2max','t2min']

    for variable in variables:
        input_dirs = [ '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/daily/{}'.format( variable ),
                        '/storage01/malindgren/wrf_ccsm4/daily/{}'.format( variable ) ]

        for input_dir, group in zip(input_dirs,range(2)):
            if 'wrf_ccsm4' in input_dir:
                models = ['NCAR-CCSM4_historical', 'NCAR-CCSM4_rcp85']
            else:
                models = ['GFDL-CM3_historical', 'GFDL-CM3_rcp85', 'ERA-Interim_historical']
            for model in models:
                modname, scenario = model.split('_')
                output_remote_dir = 's3://wrf-ak-ar5/daily/{}/{}/{}'.format( modname, scenario, variable )
                command = ' '.join(['aws','s3','cp',input_dir,output_remote_dir,'--recursive','--exclude', '"*"', ' --include', '"*{}*.nc"'.format(model)])
                fn = os.path.join( slurm_path, '{}_{}_move_s3_aws_{}_day.slurm'.format(variable, model, group) )
                run( fn, command, slurm_path, ncpus=32 )

