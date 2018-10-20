# wrap wrf variable re-stacker for running on slurm nodes
def run( fn, command, ncpus=10 ):
    import os, subprocess
    ncpus = 32 # to hold the node
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
    
    slurm_path, basename = os.path.split( fn )
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import subprocess, os

    # base directory
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'
    base_dir = '/storage01/malindgren/wrf_ccsm4/hourly'
    variables = ['ght', 'omega', 'qvapor', 'sh2o', 'smois', 't'] # fix 4D
    # ['omega', 't']
    # ['lwdnbc', 'lwupbc', 'omega', 't']

    # ['albedo', 'vegfra', 'cldfra_high',
    #  'cldfra_low', 'cldfra_mid', 'lwupbc', 'lwdnbc', 'ght', 
    #  'omega', 'psfc', 'slp', 'sh2o', 'seaice', 'swupbc', 
    #  'smois', 'swdnbc', 'tbot', 'tsk', 't']

    slurm_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/slurm'
    if not os.path.exists( slurm_dir ):
        os.makedirs( slurm_dir )
    os.chdir( slurm_dir )

    for variable in variables:
        if variable in ['acsnow','albedo','pcpt','sh2o','smois','swupbc']:
            ncpus = 5
        elif variable in ['cldfra']:
            ncpus = 3
        elif variable in ['qvapor','t','ght','omega']:
            ncpus = 1
        else:
            ncpus = 10

        command = ' '.join([ 'python', '/workspace/UA/malindgren/repos/wrf_utils/improve_hourly_netcdf_structure.py', '-b', base_dir, '-v', variable, '-n', str(ncpus) ])
        fn = os.path.join( slurm_dir, '{}_improve_hourlies.slurm'.format(variable) )
        run( fn, command )