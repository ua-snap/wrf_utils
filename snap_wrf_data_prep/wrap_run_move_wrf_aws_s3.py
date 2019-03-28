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
    # variables = ['t2','pcpt','acsnow','canwat','hfx','qvapor']
    # variables = ['cldfra_high','cldfra_mid','lwdnb','lwupb','pcpnc']
    # variables = ['potevp','q2','sh2o','smois','snowc','swdnb','swupb','tbot','tslb']
    # variables = ['albedo','cldfra','cldfra_low','lh','lwdnbc','lwupbc','pcpc','psfc']
    # variables = ['qbot','seaice','slp','snow','snowh','swdnbc','swupbc','tsk','vegfra']
    variables = ['omega','ght','t',] # RUN ALL LAST
    
    for variable in variables:
        input_dirs = [ '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/{}'.format( variable ),
                        '/storage01/malindgren/wrf_ccsm4/hourly_fix/{}'.format( variable ) ]

        for input_dir, group in zip(input_dirs,range(2)):
            if 'wrf_ccsm4' in input_dir:
                models = ['NCAR-CCSM4_historical', 'NCAR-CCSM4_rcp85']
            else:
                models = ['GFDL-CM3_historical', 'GFDL-CM3_rcp85', 'ERA-Interim_historical']
            for model in models:
                modname, scenario = model.split('_')
                output_remote_dir = 's3://wrf-ak-ar5/hourly/{}/{}/{}'.format( modname, scenario, variable )
                command = ' '.join(['aws','s3','cp',input_dir,output_remote_dir,'--recursive','--exclude', '"*"', ' --include', '"*{}*.nc"'.format(model)])
                fn = os.path.join( slurm_path, '{}_{}_move_s3_aws_{}.slurm'.format(variable, model, group) )
                run( fn, command, slurm_path, ncpus=32 )
