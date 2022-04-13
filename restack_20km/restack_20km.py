"""Functions for running the restack_20km pipeline

Notes

Glossary:
- path_like: a pathlib.Path object or string that can be interpreted as one.
"""

import os
import shutil
import subprocess
from multiprocessing import Pool
import numpy as np
import pandas as pd
import xarray as xr


# functions below this point derived from copy-to-scratch step


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
    
    wrf_dir (pathlib.PosixPath): path to the directory containing hourly WRF files
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
    
    fp1 (pathlib.PosixPath): path to first file
    fp2 (pathlib.PosixPath): path to second file to test equivalence
    head (bool): test that the "headers" of the files are the same
    detail (bool): return more info on what specfic equivalence tests failed
    
    Returns:
        bool or dict indicating equivalence of files
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


def check_raw_scratch_file(wrf_fp, group, raw_scratch_dir):
    scratch_fp = raw_scratch_dir.joinpath(group, wrf_fp.parent.name, wrf_fp.name)
    if scratch_fp.exists():
        if test_file_equivalence(wrf_fp, scratch_fp):
            return scratch_fp
        else:
            return None


def check_raw_scratch(wrf_dir, group, years, raw_scratch_dir, ncpus=24):
    """Check to see the number of requested WRF files in the raw scratch directory """
    # see if we can pool this?
    existing_scratch_fps = []
    for year in years:
        wrf_fps = get_wrf_fps(wrf_dir, [year])
        args = [(fp, group, raw_scratch_dir) for fp in wrf_fps]
        with Pool(ncpus) as pool:
            existing_scratch_fps.extend(pool.starmap(check_raw_scratch_file, args))
    # discard Nones
    existing_scratch_fps = [fp for fp in existing_scratch_fps if fp is not None]

    # get list of filenames existing on scratch
    if len(existing_scratch_fps) == 0:
        print("No files from specified years found in scratch_dir")
    else:
        existing_scratch_fns = [fp.name for fp in existing_scratch_fps]
        wrf_year_list = [int(fp.parent.name) for fp in wrf_fps]
        unique_years_tpl = np.unique(
            wrf_year_list, return_index=True, return_counts=True
        )
        # unique_years and years should be the same
        assert sorted(unique_years_tpl[0]) == sorted(years)

        # iterate over unique years indices in wrf_fps and ensure all files for that year are present
        years_on_scratch = []
        years_not_on_scratch = []
        for year, i, count in zip(*unique_years_tpl):
            idx = np.arange(i, i + count)
            year_on_scratch = np.all(
                [wrf_fps[j].name in existing_scratch_fns for j in idx]
            )
            if year_on_scratch:
                years_on_scratch.append(year)
            else:
                years_not_on_scratch.append(year)

        print(f"Years with all files present on scratch space: {years_on_scratch}")
        print(f"Years with files missing from scratch space: {years_not_on_scratch}")

    return wrf_fps, existing_scratch_fps


def make_sbatch_head(slurm_email, partition, conda_init_script, ap_env):
    """Make a string of SBATCH commands that can be written into a .slurm script
    
    Args:
        slurm_email (str): email address for slurm failures
        partition (str): name of the partition to use
        conda_init_script (path_like): path to a script that contains commands
            for initializing the shells on the compute nodes to use conda activate
        ap_env (str/path_like): path to the anaconda-project env to activate
            
    Returns:
        sbatch_head (str): string of SBATCH commands ready to be used as parameter
            in sbatch-writing functions. The following gaps are left for filling with .format:
                - ncpus
                - output slurm filename
    """
    sbatch_head = (
        "#!/bin/sh\n"
        "#SBATCH --nodes=1\n"
        "#SBATCH --cpus-per-task={}\n"
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


def write_sbatch_copyto_scratch(
    sbatch_fp, sbatch_out_fp, src_dir, dst_dir, cp_script, ncpus, sbatch_head
):
    """Write an sbatch script for copying WRF outputs to a scratch space
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        src_dir (path_like): path to annual directory containing hourly WRF files
            to copy
        dst_dir (path_like): path to annual directory where hourly WRF files will
            be copied
        cp_script (path_like): path to the script to be called to run the
            copy for a given subset
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable
            set of SBATCH commands with .format brackets for the sbatch output filename

    Returns:
        None, writes the commands to sbatch_fp
    """
    pycommand = f"python {cp_script} -s {src_dir} -d {dst_dir} -n {ncpus}\n"
    commands = sbatch_head.format(ncpus, sbatch_out_fp) + pycommand

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return


def write_sbatch_forecast_times(
    sbatch_fp,
    sbatch_out_fp,
    wrf_scratch_dir,
    anc_dir,
    forecast_times_script,
    ncpus,
    sbatch_head,
):
    """Write an sbatch script for executing the script to get the forecast times
    from hourly WRF files in scratch_dir
    
    Args:
        sbatch_fp (str): path to .slurm script to write sbatch commands to
        sbatch_out_fp (str): path to where sbatch stdout should be written
        wrf_scratch_dir (str): path to the directory in scratch_dir containing
            the hourly WRF output files to get forecast_time attribute from
        anc_dir (pathlib.PosixPath): path to ancillary dir for writing forecast times table
        forecast_times_script (str): path to the script to be called to
            get the date info and forecast times from files
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable
            set of SBATCH commands with .format brackets for the sbatch output filename
        
    Returns:
        None, writes the commands to sbatch_fp
    """
    pycommand = (
        f"python {forecast_times_script} -s {wrf_scratch_dir} -a {anc_dir} -n {ncpus}\n"
    )
    commands = sbatch_head.format(ncpus, sbatch_out_fp) + pycommand

    with open(sbatch_fp, "w") as f:
        f.write(commands)
    print(f"Forecast times slurm commands written to {sbatch_fp}")

    return


def write_sbatch_restack(
    sbatch_fp,
    sbatch_out_fp,
    restack_script,
    template_fp,
    anc_dir,
    restacked_dir,
    group,
    year,
    varname,
    ncpus,
    sbatch_head,
    geogrid_fp=None,
    accum=False,
):
    """Write an sbatch script for executing the re-stacking script
    for a given group and variable
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        restack_script (path_like): path to the script to be called to run the
            re-stacking
        template_fp (path_like): path to the template monthly WRF file to use for metadata
        anc_dir (pathlib.PosixPath): path to the ancillary directory that contains the forecast times tables
        restacked_dir (pathlib.PosixPath): directory to write the re-stacked data to
        group (str): WRF group to work on
        year (int): year to work on
        varname (str): name of the variable to re-stack
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable
            set of SBATCH commands with .format brackets for the sbatch output filename
        
    Returns:
        None, writes the commands to sbatch_fp
    """
    ftimes_fp = anc_dir.joinpath(f"WRFDS_forecast_time_attr_{group}.csv")
    out_fp = restacked_dir.joinpath(varname.lower(), f"{varname}_wrf_hourly_{group}_{year}.nc")
    out_fp.parent.mkdir(exist_ok=True)
    pycommand = (
        f"python {restack_script} "
        f"-y {year} "
        f"-v {varname} "
        f"-f {ftimes_fp} "
        f"-t {template_fp} "
        f"-o {out_fp} "
        f"-n {ncpus}\n"
    )
    if geogrid_fp:
        pycommand = pycommand.replace("\n", f" -g {geogrid_fp}\n")
    if accum:
        pycommand = pycommand.replace("\n", f" --accum\n")
    commands = sbatch_head.format(ncpus, sbatch_out_fp) + pycommand

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return


def make_yearly_scratch_dirs(group, years, scratch_dir):
    """Helper function to ensure all of the directories are present in
    scratch_dir for copying raw hourly WRF files
    
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


def sys_copy(args):
    """Copy function for running the copy in the pipeline instead of via slurm
    
    Args:
        args (tuple): tuple of args for unpacking for use with multiprocessing.Pool:
            (src_fp, dst_fp, clobber):
                src_fp (str): path to file to be copied
                dst_fp (str): destination path to copy file to
                clobber (str): one of "filesize", "head", or False, indicating whether to
                    clobber only after finding that file sizes are different, file headers
                    are different, or don't clobber, respectively. 

    Returns:
        output from os.system call to cp utility
    """
    src_fp, dst_fp, clobber = args
    if clobber:
        if clobber == "all":
            return os.system(f"cp {src_fp} {dst_fp}")
        if clobber == "filesize":
            if not test_file_equivalence(src_fp, dst_fp):
                return os.system(f"cp {src_fp} {dst_fp}")
            else:
                return
        elif clobber == "head":
            if not test_file_equivalence(src_fp, dst_fp, head=True):
                return os.system(f"cp {src_fp} {dst_fp}")
            else:
                return
    else:
        return os.system(f"cp -n {src_fp} {dst_fp}")


# functions below this line were developed to assist with ensuring hourly WRF files
#   were properly copied from source directories to scratch space


def get_file_size(fp):
    """Helper function to return a file's size in bytes
    
    Args:
        fp (pathlib.PosixPath): path to file
    
    Returns:
        size of file at fp in bytes (as int)
    """
    return fp.stat().st_size


def check_scratch_file_sizes(year_scratch_dir, ncpus=8):
    """Helper function that can be used to check the sizes of hourly WRF files in
    scratch_dir after batch copying is done. Helpful for finding what files 
    (if any) did not copy successfully.
    
    Args:
        year_scratch_dir (pathlib.PosixPath): path to the annual directory of
            hourly WRF files within scratch_dir
        ncpus (int): number of CPUs to use for Pooling the filesize checking
        
    Returns:
        flag_fps (list): list of filepaths that were flagged as not being one of the
            common sizes of these hourly WRF files.
    """
    fps = np.array(list(year_scratch_dir.glob("*.nc")))
    # unique file sizes determined from the CCSM historical data:
    valid_sizes = [35722464, 35722484, 36272728, 36272732, 36272748, 36272752]
    with Pool(ncpus) as pool:
        sizes = np.array(pool.map(get_file_size, fps))
    flag_fps = fps[[size not in valid_sizes for size in sizes]]
    return flag_fps


def recopy_raw_scratch_files(fps, wrf_dir, ncpus=8):
    """Re-copy the raw scratch files specified.
    
    Args:
        fps (list): list of paths to raw hourly WRF files in
            raw_scratch_dir that should be re-copied from wrf_dir
        wrf_dir (pathlib.PosixPath): path to the directory containing
            the annual subdirectories of hourly WRF outputs
        ncpus (int): number of CPUs to use for parallel copy
        
    Returns:
        None, re-copies the files
    """
    args = [(wrf_dir.joinpath(fp.parent.name, fp.name), fp) for fp in fps]
    print(args)
    with Pool(ncpus) as pool:
        _ = pool.starmap(shutil.copy, args)

    return
