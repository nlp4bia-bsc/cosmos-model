#!/bin/bash
#SBATCH --chdir .
#SBATCH -A {{user}}
#SBATCH --qos={{queue}}
#SBATCH --job-name={{job_name}}
#SBATCH --output={{out_file}}
#SBATCH --error={{err_file}}
#SBATCH --ntasks-per-node={{tasks}}
#SBATCH --nodes={{nodes}}
#SBATCH --cpus-per-task={{cpus}}
#SBATCH --partition={{partition}}
#SBATCH --time={{time}}
{{gpu_line}}
{{job_exclusive_line}}

# Load modules
{{module_purge_line}}
{{module_lines}}

# Venv logic
{{venv_line}}

python -c "from entry_script import print_installed_dependencies; print_installed_dependencies()"

# Execution of script
SRUN_ARGS=" \
    --cpus-per-task $SLURM_CPUS_PER_TASK \
    --jobid $SLURM_JOB_ID \
    "

{{previous_lines}}

srun $SRUN_ARGS {{singularity_lines}} {{exec_line}}
