def run( fn, commands, slurm_path, ncpus=5 ):
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
        f.write( head + '\n\n' + ''.join(commands) + '\n')
    
    os.chdir( slurm_path )
    subprocess.call([ 'sbatch', fn ])
    return 1

if __name__ == '__main__':
	import subprocess, os, glob

	slurm_path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/aws_move_slurm'
	output_remote_dir = 's3://wrf-ak-ar5/hourly'
	variables = ['t2','pcpt', 'cldfra_high', 'cldfra_mid', 'acsnow', 'canwat', 'cldfra_low', 
	'cldfra', 'pcpc', 'psfc', 'hfx', 'lwdnb', 'qbot', 'seaice', 'snow', 'slp', 'snowh', 'swdnbc', 
	'swupbc', 'tbot', 'albedo', 'lh', 'tslb', 'ght', 'potevp', 'pcpnc', 'lwupb', 'q2', 'qvapor', 
	'sh2o', 'snowc', 'smois', 'tsk', 'swdnb', 'swupb', 'vegfra', 'lwupbc', 'lwdnbc', 'omega', 't']
	
	for variable in variables:
		input_dirs = [ '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/{}'.format( variable ), 
						'/storage01/malindgren/wrf_ccsm4/hourly_fix/{}'.format( variable ) ]

		for input_dir, group in zip(input_dirs,range(2)):
			# list the files we want to move
			files = glob.glob(os.path.join(input_dir,'*_fix*.nc'))

			# change the filenames and build the proper output_dir for the files to be dumped into
			commands = []
			for fn in files:
				new_base = os.path.basename(fn).replace('_fixleap.nc','.nc')
				if 'ERA-Interim' in fn:
					model = 'ERA-Interim'
					scenario = 'historical'
				elif 'GFDL-CM3' in fn and 'historical' in fn:
					model = 'GFDL-CM3'
					scenario = 'historical'
				elif 'GFDL-CM3' in fn and 'rcp85' in fn:
					model = 'GFDL-CM3'
					scenario = 'rcp85'
				elif 'NCAR-CCSM4' in fn and 'historical' in fn:
					model = 'NCAR-CCSM4'
					scenario = 'historical'
				elif 'NCAR-CCSM4' in fn and 'rcp85' in fn:
					model = 'NCAR-CCSM4'
					scenario = 'rcp85'

				out_fn = '/'.join([output_remote_dir, model, scenario, variable, new_base])
				command = ' '.join(['aws','s3','cp',fn,out_fn])+'\n'
				commands = commands + [command]
			
			# run the commands
			fn = os.path.join( slurm_path, '{}_{}_{}_move_s3_aws.slurm'.format(variable, model, scenario) )
			run( fn, commands, slurm_path, ncpus=3 )
