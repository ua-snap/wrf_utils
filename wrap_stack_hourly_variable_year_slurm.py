# wrap wrf variable re-stacker for running on slurm nodes
def run_model( fn, command ):
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
        f.writelines( head + '\n' + command + '\n' )
    dirname, basename = os.path.split( fn )
    os.chdir( dirname ) 
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import os, subprocess

    input_path = '/storage01/pbieniek/erain/hourly'
    years = range(1979, 2015+1)
    group = 'erain'
    variables = ['PCPT', 'QBOT', 'QVAPOR', 'SNOW', 'SNOWC', 'T2', 'TSLB', 'VEGFRA']
    files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/hourly'

    for variable in variables:
        print( variable )
        for year in years:
            template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( variable )
            try:
                _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )  
            except:
                template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( 'PCPT' )
                pass

            for year in years:
                output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
                command = ' '.join['python3','/workspace/UA/malindgren/repos/wrf_utils/stack_hourly_variable_year.py', '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, '-o', output_filename, '-t', template_fn])
            
                slurm_path = os.path.join( output_path, 'slurm_log' )
                if not os.path.exists( slurm_path ):
                    os.makedirs( slurm_path )

                fn = os.path.join( slurm_path, 'run_{}_wrf_hourly_{}_{}.slurm'.format( variable, group, year ) )
                done = run_model( fn, command )
        