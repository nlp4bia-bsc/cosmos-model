#!/bin/bash
#SBATCH -A {{user}}
#SBATCH --qos={{queue}}
#SBATCH --job-name={{job_name}}
#SBATCH --output={{out_file}}
#SBATCH --error={{err_file}}
#SBATCH --ntasks=1
#SBATCH --nodes={{nodes}}
#SBATCH --cpus-per-task={{cpus}}
#SBATCH --partition={{partition}}
{{gpu_line}}
{{job_exclusive_line}}

# Load modules
{{module_lines}}

# Venv logic
{{venv_line}}

python -c "from entry_script import print_installed_dependencies; print_installed_dependencies()"

# Execution of script
{{exec_line}}
