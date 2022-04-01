"""Functions for assisting with copying WRF outputs to a scratch filesystem"""

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


def get_wrf_fps(wrf_dir, group, years):
    """Make the WRF filepaths based on the WRF dir, group,
    and 
    
    """
    if years is None:
        years = luts.groups[group].years

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


def sbatch_copyto_scratch(
    wrf_dir,
    group,
    years,
    scratch_dir,
    cp_script,
    slurm_email,
    conda_init_script,
    ap_env,
    ncpus=5,
):
    """Submit batch jobs to copy the required 
    WRF output files from wrf_dir to scratch_dir
    
    Args:
        wrf_dir (pathlib.PosixPath): path to the directory containing the
            annually-grouped directories of hourly WRF outputs
        group (str): name of the WRF group being worked on
        years (list): list of years to be copied
        scratch_dir (PosixPath): path to scratch space where WRF files 
            will be copied
        cp_script (str/path-like): path to the script to be called to run the
            copy for a given subset
        slurm_email (str): email address for Slurm failures
        conda_init_script (str/path-like): path to a script that contains commands
            for initializing the shells on the compute nodes to use conda activate
        ap_env (str/pathlike): path to the anaconda-project env to activate
        ncpus (int): number of CPUS to use for multiprocessing

    Returns:
        sbatch_fps: list of paths to the slurm sbatch job scripts
    """
    # make yearly directories in scratch_dir as needed
    group_dir = scratch_dir.joinpath(group)
    _ = [
        group_dir.joinpath(str(year)).mkdir(exist_ok=True, parents=True)
        for year in years
    ]
    # make slurm dir for the copy
    slurm_dir = scratch_dir.joinpath("slurm/cp_scratch")
    slurm_dir.mkdir(exist_ok=True, parents=True)

    # setup commands
    head = (
        "#!/bin/sh\n"
        "#SBATCH --nodes=1\n"
        f"#SBATCH --cpus-per-task={ncpus}\n"
        "#SBATCH --account=snap\n"
        "#SBATCH --mail-type=FAIL\n"
        f"#SBATCH --mail-user={slurm_email}\n"
        "#SBATCH -p t1small\n"
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

    # iterate over years and add command to execute the script with python
    sbatch_fps = []
    for year in years:
        # write to .slurm script
        sbatch_fp = slurm_dir.joinpath(f"cp_scratch_{group}_{year}.slurm")
        # filepath for slurm stdout
        sbatch_out_fp = slurm_dir.joinpath(f"cp_scratch_%j_{group}_{year}.out")
        src_dir = wrf_dir.joinpath(str(year))
        dst_dir = group_dir.joinpath(str(year))
        pycommand = f"python {cp_script} -s {src_dir} -d {dst_dir} -n {ncpus}\n"
        commands = head.format(sbatch_out_fp) + pycommand

        with open(sbatch_fp, "w") as f:
            f.write(commands)

        sbatch_fps.append(sbatch_fp)

    return sbatch_fps
