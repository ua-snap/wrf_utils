"""Copy the hourly raw outputs from $ARCHIVE to $SCRATCH_DIR for the supplied $WRF_GROUP"""

from multiprocessing import Pool
import tqdm
from config import *
import luts
import restack_20km as main


def rsync(args):
    src, dst = args
    os.system(f"rsync -a {src} {dst}")


if __name__ == "__main__":
    # set up directories and path variables
    years = luts.groups[group]["years"]
    wrf_dir = Path(luts.groups[group]["directory"])
    # make sure files are staged
    unstaged_fps = main.check_staged(wrf_dir, years)
    if len(unstaged_fps) != 0:
        exit("Not all files are staged")
    
    group_dir = raw_scratch_dir.joinpath(group)
    main.make_yearly_scratch_dirs(group, years, raw_scratch_dir)
    slurm_copy_dir = slurm_dir.joinpath("copy_raw")
    slurm_copy_dir.mkdir(exist_ok=True)
    
    args = [(wrf_dir.joinpath(str(year)), group_dir) for year in years] 
    with Pool(20) as pool:
        for _ in tqdm.tqdm(pool.imap_unordered(rsync, args), total=len(args)):
            pass
    del pool
    
    print(f"Done, raw WRF outputs are available in {group_dir}")
