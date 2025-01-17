from typing import List, Optional

from cosmos.utils import load_template


def create_slurm_script(
    job_name: str,
    queue: str,
    user: str,
    out_file: str,
    err_file: str,
    cpus: int,
    partition: str,
    nodes: int,
    exec_line: str,
    gpus: int = 0,
    job_exclusive: bool = False,
    modules: Optional[List[str]] = None,
    venv_path: Optional[str] = None,
) -> str:
    """
    Generates the content of a SLURM script using a predefined template.

    This function fills a SLURM script template with provided job-specific parameters
    and returns the resulting script content as a string.

    Parameters:
    ----------
    job_name : str
        The name of the SLURM job.
    queue : str
        The SLURM queue to submit the job to.
    user : str
        The username submitting the job.
    out_file : str
        The path to the file where the standard output of the job will be saved.
    err_file : str
        The path to the file where the standard error of the job will be saved.
    cpus : int
        The number of CPUs allocated for the job.
    partition : str
        The SLURM partition to use.
    nodes : int
        The number of nodes allocated for the job.
    exec_line : str
        The command to execute as part of the job.
    gpus : int, optional
        The number of GPUs required for the job. Defaults to 0.
    job_exclusive : bool, optional
        Whether to allocate the job in exclusive mode. Defaults to False.
    modules : Optional[List[str]], optional
        A list of modules to load in the SLURM script. Defaults to None.
    venv_path : Optional[str], optional
        The path to the virtual environment to activate before running the job. Defaults to None.

    Returns:
    -------
    str
        The content of the SLURM script with the provided parameters filled in.

    Notes:
    ------
    - The function assumes a SLURM template file named `slurm_template.sh` exists
      and can be loaded via the `load_template` function.
    - Optional parameters like GPUs, job exclusivity, and modules are included
      conditionally in the script based on their values.
    """
    template_content = load_template("slurm_template.sh")

    module_lines = "\n".join([f"module load {m}" for m in modules]) if modules else ""

    gpu_line = f"#SBATCH --gres=gpu:{gpus}" if gpus > 0 else ""
    job_exclusive_line = "#SBATCH --exclusive" if job_exclusive else ""

    # Venv logic
    venv_line = f"source {venv_path}/bin/activate"

    script_filled = (
        template_content
        .replace("{{queue}}", queue)
        .replace("{{user}}", user)
        .replace("{{job_name}}", job_name)
        .replace("{{out_file}}", out_file)
        .replace("{{err_file}}", err_file)
        .replace("{{nodes}}", str(nodes))
        .replace("{{cpus}}", str(cpus))
        .replace("{{gpu_line}}", gpu_line)
        .replace("{{job_exclusive_line}}", job_exclusive_line)
        .replace("{{partition}}", partition)
        .replace("{{module_lines}}", module_lines)
        .replace("{{venv_line}}", venv_line)
        .replace("{{exec_line}}", exec_line)
    )

    return script_filled
