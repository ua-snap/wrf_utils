# wrap wrf variable re-stacker for running on slurm nodes
def run( fn, command ):
    import os, subprocess
    head = '#!/bin/sh\n' + \
            '#SBATCH --ntasks=32\n' + \
            '#SBATCH --nodes=1\n' + \
            '#SBATCH --ntasks-per-node=32\n' + \
            '#SBATCH --account=snap\n' + \
            '#SBATCH --mail-type=FAIL\n' + \
            '#SBATCH --mail-user=malindgren@alaska.edu\n' + \
            '#SBATCH -p main\n'
    
    with open( fn, 'w' ) as f:
        f.write( head + '\n' + commands + '\n' )
    
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
if not os.path.exist( slurm_dir ):
	os.makedirs( slurm_dir )

for variable in variables:
    command = ' '.join([ 'python', 'improve_hourly_netcdf_structure.py', '-b', base_dir, '-v', variable ])
    fn = os.path.join( slurm_dir, '{}_improve_hourlies.slurm'.format(variable) )