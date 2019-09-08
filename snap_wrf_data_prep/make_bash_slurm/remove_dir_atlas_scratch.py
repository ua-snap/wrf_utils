if __name__ == '__main__':
    import os
    import argparse

    # parse some args
    parser = argparse.ArgumentParser( description='stack the hourly outputs from raw WRF outputs to NetCDF files of hourlies broken up by year.' )
    parser.add_argument( "-i", "--input_path", action='store', dest='input_path', type=str, help="input hourly directory to be removed from atlas_scratch" )
    
    # parse the args and unpack
    args = parser.parse_args()
    input_path = args.input_path    

    print(input_path)
    if 'atlas_scratch' in input_path:
        # os.system( 'rm -r {}'.format(input_path) )
        os.system( 'rm -r {}'.format(input_path) )
    else:
        print('WRONG DIRECTORY TO REMOVE!!! atlas_scratch only!')
