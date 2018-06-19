# wrap wrf variable re-stacker for running on slurm nodes
def run( fn, command ):
    import os, subprocess
    ncpus = 10 # less for this application...
    head = '#!/bin/sh\n' + \
            '#SBATCH --ntasks={}\n'.format(ncpus) + \
            '#SBATCH --nodes=1\n' + \
            '#SBATCH --ntasks-per-node={}\n'.format(ncpus) + \
            '#SBATCH --account=snap\n' + \
            '#SBATCH --mail-type=FAIL\n' + \
            '#SBATCH --mail-user=malindgren@alaska.edu\n' + \
            '#SBATCH -p main\n'
    
    with open( fn, 'w' ) as f:
        f.write( head + '\n' + command + '\n' )
    
    slurm_path, basename = os.path.split( fn )
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import subprocess, os

    # base directory
    base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'
    variables = [ i.upper() for i in os.listdir( base_dir ) ]

    slurm_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data'
    if not os.path.exists( slurm_dir ):
        os.makedirs( slurm_dir )
    os.chdir( slurm_dir )

    for variable in variables:
        command = ' '.join([ 'python', '/workspace/UA/malindgren/repos/wrf_utils/improve_hourly_netcdf_structure.py', '-b', base_dir, '-v', variable ])
        fn = os.path.join( slurm_dir, '{}_improve_hourlies.slurm'.format(variable) )
        run( fn, command )