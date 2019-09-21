def run( fn, command, slurm_path, ncpus=32 ):
    import os, subprocess
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
    
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
	import subprocess, os

	slurm_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/aws_move_slurm'
	# variables = ['t2','pcpt','t]
	# variables = ['acsnow','canwat','cldfra_high','cldfra_mid','hfx','lwdnb','lwupb','pcpnc']
	# variables = ['potevp','q2','sh2o','smois','snowc','swdnb','swupb','tbot','tslb']
	# variables = ['albedo','cldfra','cldfra_low','lh','lwdnbc','lwupbc','pcpc','psfc']
	# variables = ['qbot','seaice','slp','snow','snowh','swdnbc','swupbc','tsk','vegfra']
	# variables = ['omega']
	# variables = ['qvapor','ght']
	variables = ['pcpt','t','t2','t2max','t2min']
	models = ['ERA-Interim', 'GFDL-CM3', 'NCAR-CCSM4']
	
	for variable in variables:
		for model in models:
			if model == 'ERA-Interim':
				scenarios = ['historical']
				input_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/daily/{}'.format( variable )
			else:
				scenarios = ['historical', 'rcp85']
				if model == 'NCAR-CCSM4':
					input_dir = '/storage01/malindgren/wrf_ccsm4/daily/{}'.format( variable )
				else:
					input_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/daily/{}'.format( variable )
			for scenario in scenarios:
				output_remote_dir = 's3://wrf-ak-ar5/daily/{}/{}'.format( model, scenario )
				command = ' '.join(['aws','s3','cp',input_dir,output_remote_dir,'--recursive'])

				fn = os.path.join( slurm_path, '{}_move_s3_aws_{}_{}.slurm'.format(variable, model, scenario) )
				run( fn, command, slurm_path, ncpus=32 )
