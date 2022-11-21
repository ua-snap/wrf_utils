"""Copy the hourly raw outputs from $ARCHIVE to $SCRATCH_DIR for the supplied $WRF_GROUP"""

from multiprocessing import Pool
import tqdm
from config import *
import luts
import restack_20km as main


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
    
    ncpus = 20
    clobber = "all"
    for year in years:
        src_dir = wrf_dir.joinpath(str(year))
        dst_dir = group_dir.joinpath(str(year))
        # set third arg to False for no-clobber
        args = [(fp, dst_dir.joinpath(fp.name), clobber) for fp in src_dir.glob("*.nc")]

        with Pool(ncpus) as pool:
            for _ in tqdm.tqdm(
                pool.imap_unordered(main.sys_copy, args), total=len(args), desc=f"Year: {year}"
            ):
                pass
        del pool
    
    print(f"Done, raw WRF outputs are available in {group_dir}")
