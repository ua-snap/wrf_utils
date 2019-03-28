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
        f.write( head + '\n' + commands + '\n' )
    
    slurm_path, basename = os.path.split( fn )
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
    import os, subprocess

    # input_path = '/storage01/pbieniek/erain/hourly'
    # input_path = '/storage01/pbieniek/gfdl/hist/hourly'
    input_path = '/storage01/rtladerjr/hourly'
    # years = range(1979, 2015+1)
    years = range(1970, 2006+1) 
    # years = list(range(2006,2100+1))
    # group = 'erain'
    group = 'gfdl_hist'
    # group = 'gfdl_rcp85'
    variables = ['PCPT', 'QBOT', 'QVAPOR', 'SNOW', 'SNOWC', 'T2', 'TSLB', 'VEGFRA'] # COMPLETE: []
    # variables = ['PSFC','GHT','TSK','T','TBOT','Q2'] ::rerun me::

    # MUST RUN: #,'PCPNC','PCPC','ACSNOW','SNOWH'
    # 'CLDFRA','CLDFRA_LOW','CLDFRA_MID','CLDFRA_HIGH','ALBEDO','SEAICE',
    # 'SLP','SMOIS','SWUPBC','SWDNBC','LWUPBC','LWDNBC','SWDNB','LWDNB',
    # 'SWUPB','LWUPB','CANWAT','POTEVP','SH2O']

    files_df_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_{}.csv'.format( group )
    output_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/TESTING_SLURM_WRF'

    for variable in variables:
        print( variable )
        template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( variable )
        try:
            _ = xr.open_dataset( template_fn, decode_times=False, autoclose=True )
        except:
            template_fn = '/storage01/pbieniek/erain/monthly/monthly_{}-erai.nc'.format( 'PCPT' )
            pass

        commands = []
        for year in years:
            output_filename = os.path.join( output_path, variable.lower(), '{}_wrf_hourly_{}_{}.nc'.format(variable, group, year) )
            if group == 'erain':
                output_filename = output_filename.replace( '_erain_', '_era_interim_' )
            commands = commands + [' '.join(['python3','/workspace/UA/malindgren/repos/wrf_utils/stack_hourly_variable_year.py', 
                                            '-i', input_path, '-y', str(year), '-f', files_df_fn, '-v', variable, 
                                            '-o', output_filename, '-t', template_fn])]
        
        commands = 'srun -c 32 ' + '\n\nsrun -c 32 '.join( commands )

        
        slurm_path = os.path.join( output_path, 'slurm_log' )
        if not os.path.exists( slurm_path ):
            os.makedirs( slurm_path )

        fn = os.path.join( slurm_path, 'run_{}_wrf_hourly_{}.slurm'.format( variable, group ) )

        done = run_model( fn, commands )
        break