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
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly'
    # base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_new_variables/hourly'
    # base_dir = '/rcs/project_data/wrf_new_variables/hourly/gfdl'
    # groupname = 'GFDL'
    # base_dir = '/storage01/malindgren/wrf_ccsm4/hourly'
    base_dir = '/rcs/project_data/wrf_new_variables/hourly/ccsm4'
    groupname = 'CCSM4'
    variables = ['u10','v10','ubot','vbot']
    # variables = ['tbot','ght']
    # variables = ['acsnow', 'cldfra', 'hfx', 'lh', 'lwdnb', 'lwupb',]
    # ['pcpt', 'snowc', 'vegfra', 'qbot', 't2', 'snow', 'tslb']
    # variables = ['acsnow', 'cldfra', 'hfx', 'lh', 'lwdnb', 'lwupb',]
    # , 
    # variables = ['q2', 'snowh', 'swdnb', 'swupb', 'pcpc', 'pcpnc', 'potevp',]
    # variables =  [ 'canwat', 'tbot', 'tsk', 'seaice', 'albedo', 'psfc' ] # , 'cldfra_high',
    #             'cldfra_low', 'cldfra_mid',  
    # variables = ['smois', 'swdnbc', 'swupbc', 'lwdnbc', 'lwupbc','sh2o', 'slp',]
    # variables = ['sh2o', 'smois'] # fix 4D
    # variables = ['psfc']

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
        fn = os.path.join( slurm_dir, '{}_improve_hourlies_{}.slurm'.format(variable,groupname) )
        run( fn, command )