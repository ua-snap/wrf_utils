# To serve WRF data...

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    stolen from:
    https://stackoverflow.com/questions/2104080/how-to-check-file-size-in-python
    """
    for x in ['bytes', 'KiB', 'MiB', 'GiB', 'TiB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def get_folder_size( folder ):
    files = glob.glob( os.path.join(folder, '*.nc' ))
    return sum([ os.path.getsize( fn ) for fn in files ])
    
if __name__ == '__main__':
    import os, glob
    import pandas as pd
    import numpy as np

    base_dir = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data'
    folders = [ os.path.join(base_dir,i) for i in os.listdir( base_dir ) if os.path.isdir( os.path.join(base_dir,i) ) and i.endswith('_fix') ]

    done = dict()
    for folder in folders:
        print(folder)
        # curdir = os.path.join( base_dir, folder )

        variables = [ i for i in os.listdir( folder ) if os.path.isdir( os.path.join(folder,i) ) and 'slurm' not in i ]
        done[os.path.basename(folder).split('_')[0]] = { variable:get_folder_size(os.path.join(folder,variable)) for variable in variables }


    df = pd.DataFrame(done, columns=['hourly','daily','monthly'])
    df[ 'VARIABLE GROUP TOTALS -->' ] = df.apply(np.sum,axis=1)

    totals = pd.DataFrame({'AGGREGATION GROUP TOTALS -->':{ k:sum([v[i] for i in v]) for k,v in done.items()}}).T
    totals = totals[['hourly','daily','monthly']] # order the agg groups 
    totals[ 'VARIABLE GROUP TOTALS -->' ] = df[ 'VARIABLE GROUP TOTALS -->' ].sum()

    out_df = pd.concat( [df,totals] )

    ALL_PROCESSED_DATA_TOTAL = convert_bytes(out_df[ 'VARIABLE GROUP TOTALS -->' ].sum())

    out_df = out_df.applymap( convert_bytes )
    out_df.to_csv( '/workspace/Shared/Tech_Projects/wrf_data/project_data/data_size_estimates_bob/wrf_data_processed_vars_sizes.csv', sep=',' )

    # read in a csv made from TK's 'WRF Downscaling Variable List Master' spreadsheet on GoogleSheets (UA)
    varlist = pd.read_csv('/workspace/Shared/Tech_Projects/wrf_data/project_data/data_size_estimates_bob/WRF_VARIABLE_LIST_Full.csv')
    
    # drop some entries that are NOT going to be processed variables in the end...  Dim Vars
    NOT_VARS = ['g5_lat_0','g5_lon_1','g5_rot_2','lv_DBLY3_l0','lv_DBLY3_l1','lv_ISBL2']
    varlist = varlist.drop([ count for count,i in enumerate(varlist['Variable']) if i in NOT_VARS ])

    varlist['Variable'] = varlist['Variable'].apply(lambda x: x.lower()) # make them lower-case for comparison...

    # what vars have NOT been run yet...
    not_yet_run = [ variable for variable in varlist.Variable if variable not in out_df.index.tolist() ]

    varlist_torun = varlist[varlist.Variable.isin(not_yet_run)]
    varlist_torun.to_csv( '/workspace/Shared/Tech_Projects/wrf_data/project_data/data_size_estimates_bob/WRF_VARIABLE_LIST_NotYetRun.csv' , sep=',')





