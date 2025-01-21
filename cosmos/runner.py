# runner.py

import json
import shutil
import tarfile
import tempfile
import time
import os

from datetime import datetime
from paramiko import SSHClient  # type: ignore
from typing import Any, Dict, List, Optional

from cosmos.initialization import get_global_config, get_global_ssh_client
from cosmos.slurm import create_slurm_script
from cosmos.ssh_connection import remote_command, remote_command_stream, scp_file
from cosmos.utils import (
    compute_missing_packages,
    create_local_entry_script,
    parse_pip_freeze,
    tail_file
)


def run(
    module_path: str,
    function_name: str,
    queue: str,
    user: str,
    args: Optional[List[Any]] = [],
    kwargs: Optional[Dict[str, Any]] = {},
    requirements: Optional[List[str]] = [],
    modules: Optional[List[str]] = [],
    partition: Optional[str] = None,
    nodes: int = 1,
    cpus: int = 1,
    gpus: int = 0,
    job_exclusive: bool = False,
    watch: bool = False,
    job_name: Optional[str] = None,
    custom_command: str = "python",
    venv_path: Optional[str] = None,
    outputs: Optional[List[str]] = [],
    force_install_requirements: bool = False,
    delete_files_after_execution: bool = True,
) -> Dict[str, Any]:
    """
    Executes a function using a remote SLURM job scheduler on the server.

    Parameters:
    ----------
    module_path : str
        The Python module path where the function is defined.
    function_name : str
        The name of the function to execute.
    queue : str
        The SLURM queue to use for job submission.
    user : str
        The username for SLURM job submission.
    args : list, optional
        A list of positional arguments to pass to the function.
    kwargs : dict, optional
        A dictionary of keyword arguments to pass to the function.
    requirements : list, optional
        A list of Python dependencies to install in the virtual environment.
    modules : list, optional
        A list of SLURM modules to load before job execution.
    partition : str, optional
        The SLURM partition to use. Defaults to the value in the global config or "debug".
    nodes : int, optional
        The number of nodes required for the job. Defaults to 1.
    cpus : int, optional
        The number of CPUs required per task. Defaults to 1.
    gpus : int, optional
        The number of GPUs required per task. Defaults to 0.
    job_exclusive : bool, optional
        Whether to allocate the job exclusively. Defaults to False.
    watch : bool, optional
        Whether to monitor the job execution in real-time. Defaults to False.
    job_name : str, optional
        The name of the job. If not provided, a unique name is generated.
    custom_command : str, optional
        The command to execute the Python script. Defaults to "python".
    venv_path : str, optional
        The path to the virtual environment. If not provided, a new one is created.
    outputs : list, optional
        A list of files or directories to retain after job execution.
    force_install_requirements : bool, optional
        Whether to force reinstall Python dependencies. Defaults to False.
    delete_files_after_execution : bool, optional
        Whether to delete temporary files after execution. Defaults to True.

    Returns:
    -------
    dict
        A dictionary containing job-related information, such as job ID, name, and paths.

    Raises:
    -------
    Exception
        If an error occurs during job preparation or execution.
    """
    # 1. Getting config global with ssh_client
    print(f"[cosmos.run] Preparing configuration to execute '{function_name}'")
    cosmos_cfg = get_global_config()
    ssh_client = get_global_ssh_client()
    remote_base_path = cosmos_cfg["remote_base_path"]

    # 2. Generate a unique job name
    job_id_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = f"job_{job_id_str}"

    # 3. Serialization of function in a python script
    temp_dir = tempfile.mkdtemp()
    tar_path = os.path.join(temp_dir, "project.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        folder_name = module_path.split('.')[0]
        tar.add(folder_name, arcname=folder_name)

    # 4. Copy script to remote folder
    remote_job_dir = f"{remote_base_path}/{job_name}"
    remote_command(ssh_client, f"mkdir -p {remote_job_dir}")

    remote_tar_path = f"{remote_job_dir}/project.tar.gz"
    scp_file(ssh_client, tar_path, remote_tar_path)

    out, err = remote_command(ssh_client, f"cd {remote_job_dir} && tar -xzf project.tar.gz")
    if err.strip() and "bsc/1.0" not in err:
        print("[cosmos.run] Error in unzip:", err)

    # Copy entry script
    entry_script_local = create_local_entry_script()
    remote_entry_script = f"{remote_job_dir}/entry_script.py"
    scp_file(ssh_client, entry_script_local, remote_entry_script)

    venv_path = venv_path if venv_path else f"{remote_job_dir}/{job_name}"
    # Install dependencies
    if requirements:
        prepare_venv(ssh_client, venv_path, requirements, force_install_requirements)

    # 5. Build final command
    args_json = json.dumps(args)
    kwargs_json = json.dumps(kwargs)
    final_command = (
        f"{custom_command} {remote_job_dir}/entry_script.py "
        f"{module_path} {function_name} '{args_json}' '{kwargs_json}'"
    )

    # 6. SLURM file creation
    out_file = f"{remote_job_dir}/{job_name}.out"
    err_file = f"{remote_job_dir}/{job_name}.err"

    partition = partition or cosmos_cfg.get("default_partition", "debug")

    slurm_content = create_slurm_script(
        job_name=job_name,
        queue=queue,
        user=user,
        out_file=out_file,
        err_file=err_file,
        cpus=cpus,
        gpus=gpus,
        job_exclusive=job_exclusive,
        partition=partition,
        modules=modules,
        nodes=nodes,
        exec_line=final_command,
        venv_path=venv_path,
    )

    local_slurm = tempfile.NamedTemporaryFile(suffix=".slurm", delete=False)
    try:
        local_slurm.write(slurm_content.encode('utf-8'))
        local_slurm.close()
    except Exception as e:
        local_slurm.close()
        raise e

    remote_slurm_path = f"{remote_job_dir}/{job_name}.slurm"
    scp_file(ssh_client, local_slurm.name, remote_slurm_path)

    # 7. Execute the job
    sbatch_cmd = f"cd {remote_job_dir} && sbatch {remote_slurm_path}"
    out, err = remote_command(ssh_client, sbatch_cmd)
    if err.strip() and "bsc/1.0" not in err:
        print("[Error sbatch]:", err)

    init_out_sbatch = out.split('\n')[0]
    print(f"[cosmos.run] Output remote logs: {out_file}")
    print(f"[cosmos.run] Error remote logs: {err_file}")
    print(f"[cosmos.run] {init_out_sbatch}")

    # Parse job_id
    job_id = None
    for line in out.split("\n"):
        if "Submitted batch job" in line:
            job_id = line.strip().split()[-1]

    if not job_id:
        print("Job unknown. Check the output of sbatch.")
        job_id = "unknown"

    print(f"[cosmos.run] Job {job_name} (ID: {job_id}) sent.")

    shutil.rmtree(temp_dir, ignore_errors=True)

    job_info = {
        "job_id": job_id,
        "job_name": job_name,
        "remote_job_dir": remote_job_dir,
        "out_file": out_file,
        "err_file": err_file,
        "outputs": outputs,
    }

    if watch and job_id != "unknown":
        try:
            monitor_job(ssh_client, job_id, out_file, err_file)
        except KeyboardInterrupt:
            print("[cosmos.run] Interrupt from the user (Ctrl+C).")
            print(f"[cosmos.run] Canceling job {job_id}")
            cancel_job(job_info)

        if delete_files_after_execution:
            cleanup_remote_folder(ssh_client, remote_job_dir, keep_paths=outputs)

    return job_info


def cancel_job(job: Dict[str, str]) -> None:
    """
    Cancels a job using the SLURM `scancel` command.

    This function retrieves the global SSH client and cancels the job identified
    by its job ID using the `scancel` command on the remote server.

    Parameters:
    ----------
    job : Dict[str, str]
        A dictionary containing job information. It must include the following key:
        - "job_id": str
            The SLURM job ID to cancel.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - If an error occurs while canceling the job, it will be logged to the console.
    """
    ssh_client = get_global_ssh_client()
    job_id = job["job_id"]

    cmd = f"scancel {job_id}"
    out, err = remote_command(ssh_client, cmd)

    if err.strip() and "bsc/1.0" not in err:
        print(f"[cosmos.cancel_job] Error in cancel job {job_id}: {err}")
    else:
        print(f"[cosmos.cancel_job] Job {job_id} canceled sucessfully")


def print_logs(job: Dict[str, str]) -> None:
    """
    Prints the content of the `out` and `err` log files for a specific job.

    This function retrieves the global SSH client to read and print the logs
    (both standard output and error) from the remote server for the specified job.

    Parameters:
    ----------
    job : Dict[str, str]
        A dictionary containing job information. It must include the following keys:
        - "out_file": str
            Path to the standard output log file on the remote server.
        - "err_file": str
            Path to the error log file on the remote server.
        - "job_id": str
            The ID of the job whose logs are to be printed.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - Logs are fetched from the remote server using the global SSH client.
    - If the files do not exist or are inaccessible, the function relies on the
      `read_remote_file` implementation to handle errors.
    """
    ssh_client = get_global_ssh_client()

    out_file = job["out_file"]
    err_file = job["err_file"]
    job_id = job["job_id"]

    out_content = read_remote_file(ssh_client, out_file)
    err_content = read_remote_file(ssh_client, err_file)

    print(f"[cosmos.print_logs] Printing logs of job {job_id}")

    print("\n[cosmos.print_logs] out_file logs:")
    print(out_content)

    print("\n[cosmos.print_logs] err_file logs:")
    print(err_content)


def check_status(job: Dict[str, str], delete_files_after_execution: bool = True) -> str:
    """
    Returns the status of a job and deletes remote files if the job is finished.

    This function checks the status of a job using SLURM's `squeue` command.
    If the job is finished (COMPLETED, FAILED, CANCELLED, TIMEOUT, or DONE),
    it can optionally delete the associated files from the remote server.

    Parameters:
    ----------
    job : Dict[str, str]
        A dictionary containing job information. It must include the following keys:
        - "job_id": str
            The ID of the job whose status is being checked.
        - "remote_job_dir": str
            The remote directory associated with the job.

    delete_files_after_execution : bool, optional
        Whether to delete the remote job files if the job is finished. Defaults to True.

    Returns:
    -------
    str
        The current status of the job as reported by `squeue` or "DONE" if the job
        is no longer in the SLURM queue.

    Notes:
    ------
    - If the job is finished and `delete_files_after_execution` is True,
      the remote job directory will be cleaned up.
    - The function assumes the use of the global SSH client for remote operations.
    """
    ssh_client = get_global_ssh_client()
    job_id = job["job_id"]
    remote_job_dir = job["remote_job_dir"]

    squeue_cmd = f"squeue --job={job_id} -o '%T' --noheader"
    out_stat, _ = remote_command(ssh_client, squeue_cmd)
    job_state = out_stat.strip() if out_stat else "DONE"

    print(f"[cosmos.check_status] Job {job_id} status: {job_state}")

    if (
        job_state in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]
        or ("DONE" in job_state and delete_files_after_execution)
    ):
        cleanup_remote_folder(ssh_client, remote_job_dir, "check_status")

    return job_state


def monitor_job(ssh_client: SSHClient, job_id: str, out_file: str, err_file: str) -> None:
    """
    Monitors the real-time state and output of a job.

    This function continuously fetches and displays the status of a job using SLURM's
    `squeue` command. It also tails the standard output (`out_file`) and error (`err_file`)
    logs in real time, showing the job's progress until it is finished.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    job_id : str
        The ID of the job to monitor.
    out_file : str
        Path to the job's standard output log file on the remote server.
    err_file : str
        Path to the job's error log file on the remote server.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - The function terminates when the job is no longer listed in the SLURM queue.
    - Log file contents are fetched using a `tail`-like mechanism, and updates are
      displayed incrementally in the console.
    - The job's current status and execution time are updated every 5 seconds.
    """
    last_out_pos = 0
    last_err_pos = 0

    start_time = datetime.now()
    last_printed_line = None
    print()

    while True:
        current_time = datetime.now()
        # 1. Get squeue status
        squeue_cmd = f"squeue --job={job_id} -o '%T' --noheader"
        out_stat, _ = remote_command(ssh_client, squeue_cmd)
        job_state = out_stat.strip() if out_stat else "DONE"

        # 2. Tail of out_file
        last_out_pos = tail_file(ssh_client, out_file, last_out_pos, "out_file")

        # 3. Tail of err_file
        last_err_pos = tail_file(ssh_client, err_file, last_err_pos, "err_file")

        # 4. Printing the current status
        last_printed_line = (
            f"[job {job_id}][{str(datetime.now())}] Status: {job_state} "
            f"- Time execution: {str(current_time - start_time)}"
        )
        print(last_printed_line, end='\r')

        # 5. if job is not in squeue so it is finished
        if job_state in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"] or "DONE" in job_state:
            print()
            break

        time.sleep(5)


def cleanup_remote_folder(
    ssh_client: SSHClient,
    remote_job_dir: str,
    executor: str = "run",
    keep_paths: List[str] = []
) -> None:
    """
    Deletes files and folders in a remote directory after job execution, keeping specified paths.

    This function removes all files in the remote job directory except `.out` and `.err` files
    and any paths explicitly listed in `keep_paths`. Empty directories are also removed.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    remote_job_dir : str
        The path to the remote directory associated with the job.
    executor : str, optional
        The name of the executor triggering the cleanup (default is "run").
        Used for logging purposes.
    keep_paths : List[str], optional
        A list of relative or absolute paths to exclude from deletion. Defaults to an empty list.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - Paths in `keep_paths` can be relative (to `remote_job_dir`) or absolute.
    - `.out` and `.err` files are preserved by default.
    - Errors during deletion are logged but do not interrupt the execution.
    """
    exclusions = []
    for kp in keep_paths:
        if not kp.startswith("/"):
            # Normalizamos con normpath
            kp_rel = os.path.normpath(kp)  # quita './', '../'
            kp_abs = os.path.normpath(os.path.join(remote_job_dir, kp_rel))
        else:
            kp_abs = kp

        exclusions.append(kp_abs)
        exclusions.append(f"{kp_abs}/*")

    exclude_str = " ".join([f"! -path '{e}'" for e in exclusions])
    cmd_delete = (
        f"find {remote_job_dir} -type f "
        f"! -name '*.out' ! -name '*.err' "
        f"{exclude_str} "
        f"-delete"
    )
    out, err = remote_command(ssh_client, cmd_delete)
    if err.strip() and "bsc/1.0" not in err:
        print(f"[cosmos.{executor}] Error deleting files:", err)

    cmd_delete_dirs = f"find {remote_job_dir} -type d -empty -delete"
    out, err = remote_command(ssh_client, cmd_delete_dirs)
    if err.strip() and "bsc/1.0" not in err:
        print("[cosmos.run] Error in empty folders:", err)

    print(f"[cosmos.{executor}] Job finished.")
    print(f"[cosmos.{executor}] Check logs in {remote_job_dir}")


def read_remote_file(ssh_client: SSHClient, remote_path: str) -> Optional[str]:
    """
    Reads the content of a remote file via SFTP.

    This function uses an SFTP connection to open and read the contents of a file
    located on a remote server.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    remote_path : str
        The full path to the file on the remote server.

    Returns:
    -------
    Optional[str]
        The content of the remote file as a UTF-8 decoded string if the file exists
        and is readable. If the file does not exist or cannot be read, an error
        message string is returned instead.

    Notes:
    ------
    - The function uses `paramiko.SFTPClient` to handle file operations.
    - If the file cannot be accessed, an error message is returned rather than raising an exception.
    """
    try:
        sftp = ssh_client.open_sftp()
        with sftp.open(remote_path, 'r') as f:
            data = f.read().decode('utf-8')

        sftp.close()
        return data
    except IOError:
        return (
            f"[cosmos.read_remote_file] File {remote_path}"
            "does not exists or it is able to read."
        )


def prepare_venv(
    ssh_client: SSHClient,
    venv_path: str,
    requirements: Optional[List[str]] = None,
    force_install_requirements: bool = False
) -> None:
    """
    Creates or updates a virtual environment on a remote server.

    This function ensures that a Python virtual environment exists at the specified
    path on the remote server. If a list of Python package requirements is provided,
    it installs missing packages. Optionally, it can force the reinstallation of all
    requirements.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    venv_path : str
        The path where the virtual environment should be created or exists.
    requirements : Optional[List[str]], optional
        A list of Python packages to install in the virtual environment. Defaults to None.
    force_install_requirements : bool, optional
        Whether to force the reinstallation of the specified requirements. Defaults to False.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - If the virtual environment does not exist at the specified path, it is created.
    - If `requirements` is not provided, the virtual environment is prepared without
      installing any packages.
    - If `force_install_requirements` is True, all specified packages are reinstalled
      regardless of their current state.
    - Logs any errors encountered during the creation or package installation processes.
    """
    print("[cosmos.run] Creating virtual environment")
    cmd_create_venv = f"""
if [ ! -f {venv_path}/bin/activate ]; then
    echo "[prepare_venv] Virtual env NOT found, creating at {venv_path}"
    python -m venv {venv_path}
else
    echo "[prepare_venv] Virtual env ALREADY exists at {venv_path}"
fi
"""
    out, err = remote_command(ssh_client, cmd_create_venv)

    if err.strip() and "bsc/1.0" not in err:
        print("[cosmos.run] Error in venv creation", err)

    if not requirements:
        print("[cosmos.run] No requirements. Environment ready")
        return

    if force_install_requirements:
        joined_reqs = " ".join(requirements)
        print("[cosmos.run] Forcing instalation of", joined_reqs)
        cmd_install = (
            f"source {venv_path}/bin/activate && "
            f"pip install --upgrade pip && "
            f"pip install {joined_reqs}"
        )
        remote_command_stream(ssh_client, cmd_install)

    # In case force_install_requirements is False then we check the dependencies
    # before install new dependencies
    cmd_freeze = f"source {venv_path}/bin/activate && pip freeze --disable-pip-version-check"
    out, err = remote_command(ssh_client, cmd_freeze)
    if err.strip() and "bsc/1.0" not in err:
        print("[cosmos.run] Error in pip freeze:", err)
        installed_packages = {}
    else:
        installed_packages = parse_pip_freeze(out)

    missing = compute_missing_packages(requirements, installed_packages)

    if missing:
        joined_missing = " ".join(missing)

        print(f"[cosmos.run] Installing missing packages: {missing}")
        cmd_install_missing = (
            f"source {venv_path}/bin/activate && "
            f"pip install --upgrade pip && "
            f"pip install {joined_missing}"
        )
        remote_command_stream(ssh_client, cmd_install_missing)

    else:
        print("[cosmos.run] All requirements already installed")

    print(f"[cosmos.run] Environment ready in {venv_path}. Requirements: {requirements}")
