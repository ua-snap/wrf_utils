def write_batch(fn, variable, output_filename, year, input_path, dione_path):
    head = \
    "#!/bin/sh\n"+\
    "#SBATCH --nodes=1\n"+\
    "#SBATCH --cpus-per-task=32\n"+\
    "#SBATCH --account=snap\n"+\
    "#SBATCH --mail-type=FAIL\n"+\
    "#SBATCH --mail-user=malindgren@alaska.edu\n"+\
    "#SBATCH -p main\n\n"

    args = "SCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year.py\n"+\
            "DIONEPATH={}\n".format(dione_path)+\
            "INPATH={}\n".format(input_path)+\
            "FILES_DF_FN=/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv\n" +\
            "VARIABLE={}\n".format(variable) +\
            "OUTPUT_FILENAME={}\n".format(output_filename) +\
            "TEMPLATE_FN=/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_PCPT-gfdlh.nc\n"+\
            "YEAR={}\n\n".format(year)+\
            "ipython ${SCRIPTNAME} -- -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN}\n"

    with open( fn, 'w' ) as f:
        f.write( head + '\n' + args + '\n' )
    return fn

def write_batch_winds(fn, variable, output_filename, year, input_path, dione_path):
    head = \
    "#!/bin/sh\n"+\
    "#SBATCH --nodes=1\n"+\
    "#SBATCH --cpus-per-task=32\n"+\
    "#SBATCH --account=snap\n"+\
    "#SBATCH --mail-type=FAIL\n"+\
    "#SBATCH --mail-user=malindgren@alaska.edu\n"+\
    "#SBATCH -p main\n\n"

    args = "SCRIPTNAME=/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/stack_hourly_variable_year_winds.py\n"+\
            "DIONEPATH=\n".format(dione_path)+\
            "INPATH={}\n".format(input_path)+\
            "FILES_DF_FN='/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf/docs/WRFDS_forecast_time_attr_gfdl_rcp85.csv\n" +\
            "VARIABLE={}\n".format(variable) +\
            "OUTPUT_FILENAME={}\n".format(output_filename) +\
            "TEMPLATE_FN=/atlas_scratch/malindgren/WRF_DATA/ANCILLARY/monthly/monthly_PCPT-gfdlh.nc\n"+\
            "YEAR={}\n".format(year)+\
            "ANCILLARY_FN=/workspace/Shared/Tech_Projects/wrf_data/project_data/ancillary_wrf_constants/geo_em.d01.nc\n\n"+\
            "ipython ${SCRIPTNAME} -- -i ${INPATH} -id ${DIONEPATH} -y ${YEAR} -f ${FILES_DF_FN} -v ${VARIABLE} -o ${OUTPUT_FILENAME} -t ${TEMPLATE_FN} -a ${ANCILLARY_FN}\n"

    with open( fn, 'w' ) as f:
        f.write( head + '\n' + args + '\n' )
    return fn


if __name__ == '__main__':
    import itertools, os

    # some set up args
    groups = ['gfdl_rcp85','ccsm_hist', 'erain', 'ccsm_rcp85', 'gfdl_hist']
    for group in groups:
        dione_paths = {'gfdl_rcp85':'/storage01/rtladerjr/hourly', 
                    'ccsm_rcp85':'/storage01/pbieniek/ccsm/rcp85/hourly', 
                    'gfdl_hist':'/storage01/pbieniek/gfdl/hist/hourly', 
                    'ccsm_hist':'/storage01/pbieniek/ccsm/hist/hourly', 
                    'erain':'/storage01/pbieniek/erain/hourly'}

        year_ranges = {'gfdl_rcp85':(2006,2100), 'ccsm_rcp85':(2005,2100),
                    'gfdl_hist':(1970,2006), 'ccsm_hist':(1970,2005), 'erain':(1979,2018)}

        begin,end = year_ranges[group]

        dione_path=dione_paths[group]
        input_path='/atlas_scratch/malindgren/WRF_DATA/{}'.format(group)
        output_path = '/rcs/project_data/WRF_DATA_SEP2019'
        years = range(begin, end+1)
        variables = ['ACSNOW', 'T2', 'CANWAT', 'CLDFRA', 'HFX', 'LH', 'LWDNB', 'LWUPB', 'PCPC', 'PCPNC', 'PCPT', 'POTEVP', 
                'QBOT', 'Q2', 'SNOW', 'QVAPOR', 'SNOWC', 'SNOWH', 'SWUPB', 'SWDNB', 'TSLB', 'ALBEDO', 'VEGFRA', 
                'CLDFRA_HIGH', 'CLDFRA_LOW', 'CLDFRA_MID', 'LWUPBC', 'LWDNBC', 'GHT', 'OMEGA', 'PSFC', 'SLP', 'SH2O', 
                'SEAICE', 'SWUPBC', 'SMOIS', 'SWDNBC', 'TBOT', 'TSK', 'T', 'U', 'V', 'U10', 'V10', 'UBOT', 'VBOT']

        # write the batch file
        for year, variable in itertools.product(years, variables):
            output_filename = os.path.join(output_path,variable.lower(),'{}_wrf_hourly_{}_{}.nc'.format(variable, group, year))
            fn = '/workspace/UA/malindgren/repos/wrf_utils/snap_wrf_data_prep/make_bash_slurm/slurm_scripts/{1}/{0}_{1}_{2}.slurm'.format(variable,year,group)
            print('writing:{}'.format(os.path.basename(fn)))
            # make sure there are dirs to dump the files
            dirname = os.path.dirname(fn)
            if not os.path.exists(dirname):
                _ = os.makedirs(dirname)

            if variable in ['U','V','U10','V10','UBOT','VBOT']:
                _ = write_batch_winds(fn, variable, output_filename, year, input_path, dione_path)
            else:
                _ = write_batch(fn, variable, output_filename, year, input_path, dione_path)

