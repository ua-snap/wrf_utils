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

    # # base directory
    base_dir = '/rcs/project_data/wrf_data/hourly'
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
    # base_dir = '/rcs/project_data/wrf_data/hourly'
    # groupname = 'GFDL'
    # base_dir = '/storage01/malindgren/wrf_ccsm4/hourly'
    # base_dir = '/rcs/project_data/wrf_new_variables/hourly/ccsm4'
    # groupname = 'CCSM4'
    variables = ['tsk','t2']


    slurm_dir = '/rcs/project_data/wrf_data/slurm'
    if not os.path.exists( slurm_dir ):
        os.makedirs( slurm_dir )
    os.chdir( slurm_dir )

    for variable in variables:
        if variable in ['acsnow','albedo','pcpt','sh2o','smois','swupbc']:
            ncpus = 5
        elif variable in ['cldfra']:
            ncpus = 3
        elif variable in ['qvapor','t','ght','omega', 'u', 'v']:
            ncpus = 1
        else:
            ncpus = 10

        command = ' '.join([ 'python', '/workspace/UA/malindgren/repos/wrf_utils/improve_hourly_netcdf_structure.py', '-b', base_dir, '-v', variable, '-n', str(ncpus) ])
        fn = os.path.join( slurm_dir, '{}_improve_hourlies_{}.slurm'.format(variable,'version_1_update') )
        run( fn, command )