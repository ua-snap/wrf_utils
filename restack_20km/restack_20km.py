"""Functions for running the restack_20km pipeline"""

import subprocess
import xarray as xr


def validate_group(wrf_dir, group):
    """Check that the WRF directory matches the group
    """
    parent = wrf_dir.parent
    model, scenario = group.split("_")

    #   a bit wonky because in the current setup,
    # ERA-Interim is the only model that does not
    # have a "scenario" subfolder
    if model == "erain":
        if parent.name == model:
            return True
        else:
            return False
    elif scenario != parent.name:
        return False
    elif model != parent.parent.name:
        return False
    else:
        return True


def get_wrf_fps(wrf_dir, years):
    """Get all of the hourly WRF output filepaths for given
    wrf directory and years
    
    wrf_dir (pathlib.PosixPath): path to the directory containing daily WRF files
        of hourly outputs
    years (list): list of years to get filepaths for
    
    Returns:
        wrf_fps (list): list of WRF filepaths
    """
    wrf_fps = []
    for year in years:
        wrf_fps.extend(wrf_dir.joinpath(str(year)).glob("*.nc"))

    return wrf_fps


def test_file_equivalence(fp1, fp2, heads=False, detail=False):
    """Check that fp1 and fp2 have the same 
    size (and "header" info if heads), 
    return bool (or dict of bools if detail)
    """
    if fp1 == fp2:
        raise ValueError("fp1 and fp2 must be different")

    # check file sizes
    try:
        sizes_equal = fp1.stat().st_size == fp2.stat().st_size
    # except error for non-existing file error
    except FileNotFoundError:
        return False

    if heads:
        # check file heads
        with xr.open_dataset(fp1) as ds1:
            with xr.open_dataset(fp2) as ds2:
                heads_equal = str(ds1) == str(ds2)

        if not detail:
            return np.all([sizes_equal, heads_equal])
        else:
            return {"sizes_equal": sizes_equal, "heads_equal": heads_equal}

    else:
        return sizes_equal
    
    
def check_raw_scratch(wrf_dir, group, years, raw_scratch_dir):
    """Check to see the number of requested WRF files in the raw scratch directory"""
    wrf_fps = get_wrf_fps(wrf_dir, years)
    existing_scratch_fps = []
    for fp in wrf_fps:
        scratch_fp = raw_scratch_dir.joinpath(group, fp.parent.name, fp.name)
        if scratch_fp.exists():
            if test_file_equivalence(fp, scratch_fp):
                existing_scratch_fps.append(scratch_fp)
    print((
        f"{len(existing_scratch_fps)} of {len(wrf_fps)} requested "
        f"WRF output files found in {raw_scratch_dir}."
    ))
    
    return wrf_fps, existing_scratch_fps



def make_sbatch_head(ncpus, slurm_email, partition, conda_init_script, ap_env):
    """Make a string of SBATCH commands that can be written into a .slurm script
    
    Args:
        ncpus (int): number of CPUS to use for multiprocessing
        slurm_email (str): email address for slurm failures
        partition (str): name of the partition to use
        conda_init_script (str/path-like): path to a script that contains commands
            for initializing the shells on the compute nodes to use conda activate
        ap_env (str/pathlike): path to the anaconda-project env to activate
            
    Returns:
        sbatch_head (str): string of SBATCH commands ready to be used as parameter
            in sbatch-writing functions
    """
    sbatch_head = (
        "#!/bin/sh\n"
        "#SBATCH --nodes=1\n"
        f"#SBATCH --cpus-per-task={ncpus}\n"
        "#SBATCH --mail-type=FAIL\n"
        f"#SBATCH --mail-user={slurm_email}\n"
        f"#SBATCH -p {partition}\n"
        "#SBATCH --output {}\n"
        # print start time
        "echo Start slurm && date\n"
        # prepare shell for using activate - Chinook requirement
        f"source {conda_init_script}\n"
        # okay this is not the desired way to do this, but Chinook compute
        # nodes are not working with anaconda-project, so we activate
        # this manually then run the python command
        f"conda activate {ap_env}\n"
    )
    
    return sbatch_head


def write_sbatch_copyto_scratch(sbatch_fp, sbatch_out_fp, src_dir, dst_dir, cp_script, ncpus, sbatch_head):
    """Write an sbatch script for copying WRF outputs to a scratch space
    
    Args:
        sbatch_fp (str/pathlike): path to .slurm script to write sbatch commands to
        sbatch_out_fp (str/pathlike): path to where sbatch stdout should be written
        src_dir (str/pathlike): path to annual directory containing daily WRF files
            of hourly outputs to copy
        dst_dir (str/pathlike): path to annual directory where daily WRF files will
            be copied
        cp_script (str/path-like): path to the script to be called to run the
            copy for a given subset
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable
            set of SBATCH commands with .format brackets for the sbatch output filename

    Returns:
        None, writes the commands to sbatch_fp
    """
    pycommand = f"python {cp_script} -s {src_dir} -d {dst_dir} -n {ncpus}\n"
    commands = sbatch_head.format(sbatch_out_fp) + pycommand

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return


def write_sbatch_restack(sbatch_fp, sbatch_out_fp, restack_script):
    """Write an sbatch script for executing the re-stacking script
    for a given group and variable
    
    Args:
        sbatch_fp (str/pathlike): path to .slurm script to write sbatch commands to
        sbatch_out_fp (str/pathlike): path to where sbatch stdout should be written
        restack_script (str/path-like): path to the script to be called to run the
            re-stacking
        
    Returns:
        None, writes the commands to sbatch_fp
    """
    pycommand = (
        f"python {restack_script} -n {ncpus}\n"
    )
    commands = sbatch_head.format(sbatch_out_fp) + pycommand

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return

        
def make_yearly_scratch_dirs(group, years, scratch_dir):
    """Helper function to ensure all of the directories are present in
    scratch_dir for copying files
    
    Args:
        group (str): name of WRF group for use in directory paths
        years (list): list of years (ints) to create subdirs for
        scratch_dir (pathlib.PosixPath): path to scratch_dir for making
            directories in
            
    Returns:
        None, makes directories if they don't exist
    """
    group_dir = scratch_dir.joinpath(group)
    _ = [
        group_dir.joinpath(str(year)).mkdir(exist_ok=True, parents=True)
        for year in years
    ]
    
    return

    
def submit_sbatch(sbatch_fp):
    """Submit a script to slurm via sbatch
    
    Args:
        sbatch_fp (pathlib.PosixPath): path to .slurm script to submit
        
    Returns:
        job id for submitted job
    """
    out = subprocess.check_output(["sbatch", str(sbatch_fp)])
    job_id = out.decode().replace("\n", "").split(" ")[-1]
    
    return job_id