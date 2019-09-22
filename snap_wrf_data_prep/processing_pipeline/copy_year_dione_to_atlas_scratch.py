def copy_fn( input_fn, output_dir ):
    return os.system('cp -R {} {}'.format(input_fn, output_dir))

def run( x ):
    return copy_fn( *x )

if __name__ == '__main__':
    import os, glob
    import multiprocessing as mp
    import argparse

    # parse some args
    parser = argparse.ArgumentParser( description='stack the hourly outputs from raw WRF outputs to NetCDF files of hourlies broken up by year.' )
    parser.add_argument( "-i", "--input_path", action='store', dest='input_path', type=str, help="input hourly directory raw directory of files to be stacked for a given year" )
    parser.add_argument( "-o", "--output_path", action='store', dest='output_path', type=str, help="output directory to dump the data temporarily" )
    # parser.add_argument( "-y", "--year", action='store', dest='year', type=int, help="year to process" )
    
    # parse the args and unpack
    args = parser.parse_args()
    input_path = args.input_path
    output_path = args.output_path
    # year = args.year

    # # # # FOR TESTING
    # input_path = '/storage01/rtladerjr/hourly/2007'
    # output_path = '/atlas_scratch/malindgren/WRF_DATA/2007'
    # # # # # # # # # # 
    
    # list the files
    files = glob.glob(os.path.join(input_path, '*.nc'))
    ncpus = 5

    # make sure the output_path is actually there...
    if not os.path.exists(output_path):
        _ = os.makedirs(output_path)

    # build args for mp
    args = [ (fn, os.path.join(output_path, os.path.basename(fn))) for fn in files ]

    # multiprocess
    p = mp.Pool(ncpus)
    out = p.map(run, args)
    p.close()
    p.join()
