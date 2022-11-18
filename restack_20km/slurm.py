"""Functions to assist with constructing slurm jobs"""

import subprocess


def make_sbatch_head(slurm_email, partition, conda_init_script):
    """Make a string of SBATCH commands that can be written into a .slurm script
    
    Args:
        slurm_email (str): email address for slurm failures
        partition (str): name of the partition to use
        conda_init_script (path_like): path to a script that contains commands for initializing the shells on the compute nodes to use conda activate
            
    Returns:
        sbatch_head (str): string of SBATCH commands ready to be used as parameter in sbatch-writing functions. The following gaps are left for filling with .format:
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
        f"conda activate wrf_utils\n"
    )

    return sbatch_head


def write_sbatch_copyto_scratch(
    sbatch_fp, sbatch_out_fp, src_dir, dst_dir, cp_script, ncpus, sbatch_head
):
    """Write an sbatch script for copying WRF outputs to a scratch space
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        src_dir (path_like): path to annual directory containing hourly WRF files to copy
        dst_dir (path_like): path to annual directory where hourly WRF files will be copied
        cp_script (path_like): path to the script to be called to run the copy for a given subset
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable set of SBATCH commands with .format brackets for the sbatch output filename

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
    """Write an sbatch script for executing the script to get the forecast times from hourly WRF files in scratch_dir
    
    Args:
        sbatch_fp (str): path to .slurm script to write sbatch commands to
        sbatch_out_fp (str): path to where sbatch stdout should be written
        wrf_scratch_dir (str): path to the directory in scratch_dir containing the hourly WRF output files to get forecast_time attribute from
        anc_dir (pathlib.PosixPath): path to ancillary dir for writing forecast times table
        forecast_times_script (str): path to the script to be called to get the date info and forecast times from files
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable set of SBATCH commands with .format brackets for the sbatch output filename
        
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
    luts_fp,
    anc_dir,
    restack_dir,
    group,
    fn_str,
    years,
    varname,
    ncpus,
    sbatch_head,
    geogrid_fp,
):
    """Write an sbatch script for executing the restacking script for a given group and variable, executes for a given list of years 
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        restack_script (path_like): path to the script to be called to run the restacking
        luts_fp (path_like): path to the luts.py file for the restack_20km pipeline
        anc_dir (pathlib.PosixPath): path to the ancillary directory that contains the forecast times tables
        restack_dir (pathlib.PosixPath): directory to write the restacked data to
        group (str): WRF group to work on
        fn_str (str): string name of model / scenario for use in output filename, e.g. "NCAR-CCSM4_historical"
        years (list): list of years to work on
        varname (str): name of the variable to restack
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable set of SBATCH commands with .format brackets for the sbatch output filename
        geogrid_fp (path_like): path to WRF geogrid file
        
    Returns:
        None, writes the commands to sbatch_fp
        
    Notes:
        since these jobs seem to take on the order of 5 minutes or less, seems better to just run through all years once a node is secured for a job, instead of making a single job for every year / variable combination
    """
    ftimes_fp = anc_dir.joinpath(f"WRFDS_forecast_time_attr_{group}.csv")
    pycommands = "\n"
    for year in years:
        out_fp = restack_dir.joinpath(varname.lower(), f"{varname.lower()}_hourly_wrf_{fn_str}_{year}.nc")
        out_fp.parent.mkdir(exist_ok=True)
        pycommands += (
            f"python {restack_script} "
            f"-y {year} "
            f"-v {varname} "
            f"-f {ftimes_fp} "
            f"-o {out_fp} "
            f"-l {luts_fp} "
            f"-n {ncpus} "
            f"-g {geogrid_fp}\n\n"
        )
    commands = sbatch_head.format(ncpus, sbatch_out_fp) + pycommands

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return


def write_sbatch_resample(
    sbatch_fp,
    sbatch_out_fp,
    resample_script,
    hourly_dir,
    daily_dir,
    # group,
    fn_str,
    year_str,
    wrf_varname,
    out_varname,
    aggr,
    ncpus,
    sbatch_head,
):
    """Write an sbatch script for executing the resampling script for a given group and variable, executes for a given range of years 
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        resample_script (path_like): path to the script to be called to run the resampling
        hourly_dir (path_like): directory to get the restacked data from, containing folders named by WRF variable
        daily_dir (path_like): path to directory containing daily data folders grouped by variable
        fn_str (str): string name of model / scenario for use in output filename, e.g. "NCAR-CCSM4_historical"
        year_str (str): String for years to process, in '<start year>-<end year>' format
        wrf_varname (str): name of the WRF variable being resampled
        out_varname (str): name of the variable to resample to (i.e., aggregate name)
        aggr (str): name of aggregation being done (e.g. "min", "mean", "max", "sum"
        ncpus (int): number of CPUS to use for multiprocessing
        sbatch_head (str): output from make_sbatch_head that generates a suitable set of SBATCH commands with .format brackets for the sbatch output filename
        
    Returns:
        None, writes the commands to sbatch_fp
        
    Notes:
        since these jobs seem to take on the order of 5 minutes or less, seems better to just run through all years once a node is secured for a job, instead of making a single job for every year / variable combination
    """
    pycommands = "\n"
    pycommands += (
        f"python {resample_script} "
        f"-hd {hourly_dir} "
        f"-d {daily_dir} "
        f"-y {year_str} "
        f"-a {aggr} "
        f"-wv {wrf_varname} "
        f"-ov {out_varname} "
        f"-n {ncpus} "
        f"-fs {fn_str}\n\n"
    )
    commands = sbatch_head.format(ncpus, sbatch_out_fp) + pycommands

    with open(sbatch_fp, "w") as f:
        f.write(commands)

    return


def write_sbatch_copy_restacked(
    sbatch_fp,
    sbatch_out_fp,
    src_dir,
    dst_dir,
    sbatch_head,
):
    """Write an sbatch script for copying files from a restacked directory to another
    
    Args:
        sbatch_fp (path_like): path to .slurm script to write sbatch commands to
        sbatch_out_fp (path_like): path to where sbatch stdout should be written
        src_dir (path_like): path to directory containing files to be copied
        dst_dir (path_like): destination directory
        sbatch_head (str): output from make_sbatch_head that generates a suitable set of SBATCH commands with .format brackets for the sbatch output filename
        
    Returns:
        None, writes the commands to sbatch_fp
    """
    commands = "\n"
    commands = sbatch_head.format(1, sbatch_out_fp) + f"\ntime cp {src_dir}/*.nc {dst_dir} "

    with open(sbatch_fp, "w") as f:
        f.write(commands)

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
