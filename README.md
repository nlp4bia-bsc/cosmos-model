# Cosmos Model Library

Cosmos Model Library is designed to facilitate the execution of Python scripts on remote servers using the SLURM workload manager. This library streamlines the process of submitting, monitoring, and managing jobs on SLURM clusters.

## Prerequisites

To ensure the library works correctly, the following requirements must be met:

1. **Environment Variables**: The virtual environment must include at least the following variables:

   - `COSMOS_SSH_USER`: The username for accessing the remote server.
   - `COSMOS_SSH_PASSWORD`: The password for accessing the remote server.

   It is recommended to store these variables in a `.env` file and load them using:

   ```bash
   source .env
   ```

## Recommended Project Structure

For the library to function correctly, the project should be organized as follows:

```
folder_principal
|_ folder_with_python_script  # Must be a Python module (contains `__init__.py`)
|_ main.py (or a Jupyter notebook) # Script for initialization and running jobs
```

`main.py` (or the notebook) should contain the necessary setup for initializing and running jobs.

## Installation

Install the library using pip:

```bash
pip install git+https://github.com/nlp4bia-bsc/cosmos-model.git
```

if you want a specific versión the library you can execute this line

```bash
pip install git+https://github.com/nlp4bia-bsc/cosmos-model.git@<TAG>
```

## Usage

### Initialization

The `initialization` function sets up the global configuration and SSH connection. It must be called before running any jobs.

**Parameters**:
- `host` (`str`): Host to execute the job
- `remote_base_path` (Optional, `str`): Path to save all job executed

**Example**:

```python
from cosmos import initialization

initialization(host="YOUR_HOST_HERE")
```

### Running a Job

The `run` function submits a Python function as a SLURM job.

**Parameters**:
- `module_path` (str): The Python module containing the function to execute.
- `function_name` (str): The name of the function to execute.
- `queue` (str): The SLURM queue to submit the job to.
- `user` (str): The user submitting the job.
- `args` (list): Positional arguments for the function.
- `kwargs` (dict): Keyword arguments for the function.
- Additional parameters for SLURM configuration such as `cpus`, `gpus`, `partition`, etc.

**Example**:

```python
from cosmos import run

job_info = run(
    module_path="folder_with_python_script.module",
    function_name="my_function",
    queue="debug",
    user="my_user",
    args=["arg1"],
    kwargs={"key": "value"},
    cpus=2,
    gpus=1
)
```

### Canceling a Job

The `cancel_job` function cancels a submitted SLURM job.

**Parameters**:
- `job` (dict): A dictionary containing job information, including `job_id`.

**Example**:

```python
from cosmos import cancel_job

cancel_job(job_info)
```

### Printing Logs

The `print_logs` function retrieves and prints the logs of a job.

**Parameters**:
- `job` (dict): A dictionary containing job information, including `out_file` and `err_file`.

**Example**:

```python
from cosmos import print_logs

print_logs(job_info)
```

### Checking Job Status

The `check_status` function checks the current status of a SLURM job.

**Parameters**:
- `job` (dict): A dictionary containing job information, including `job_id` and `remote_job_dir`.
- `delete_files_after_execution` (bool): Whether to clean up job files after completion. Default is `True`.

**Example**:

```python
from cosmos import check_status

status = check_status(job_info)
print(f"Job status: {status}")
```

## Example Workflow

Here is an example workflow:

1. Create your Python script (e.g., `main.py`):
   ```python
   from cosmos import initialization, run, print_logs, check_status

   # Initialize the configuration and SSH connection
   initialization(host="<YOUR_HOST_HERE")

   # Run a job
   job_info = run(
       module_path="my_module",
       function_name="my_function",
       queue="debug",
       user="user",
       args=["arg1"],
       kwargs={"key": "value"},
       cpus=2
   )

   # Monitor job status
   status = check_status(job_info)
   print(f"Job status: {status}")

   # Print job logs
   print_logs(job_info)
   ```

3. Execute your script:
   ```bash
   python main.py
   ```

## Notes

- Ensure the SLURM cluster and SSH configurations are properly set up.
- The library assumes a functional SLURM environment on the remote server.

## License

This library is open source and developed by the NLPBIA team at the Barcelona Supercomputing Center.

### Contributors
- Pablo Arancibia
