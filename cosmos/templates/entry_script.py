# entry_script.py

import sys
import json
import importlib
import subprocess

"""
Usage:
    python entry_script.py module function '["arg1", "arg2", ...]' '{"kwargs1: val1, ...}'

Example:
    python entry_script.py module.functions test "[5, 10]" "{}"

This will import: from my_module.functions import test
Then will call: test(*args, **kwargs)
"""


def print_installed_dependencies():
    """
    Print all the installed dependencies in the actual environment
    """
    try:
        result = subprocess.run(
            ["pip", "freeze"],
            capture_output=True,
            text=True,
            check=True
        )
        print("=== Dependencies installed ===")
        print(result.stdout)
        print("=========================================")
    except Exception as e:
        print("Error executing 'pip freeze':", str(e))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "Usage: python entry_script.py <module_path> "
            "<function_name> <args_json> [<kwargs_json>]"
        )
        sys.exit(1)

    module_path = sys.argv[1]
    function_name = sys.argv[2]
    args_json = sys.argv[3]
    kwargs_json = sys.argv[4] if len(sys.argv) > 4 else "{}"

    args = json.loads(args_json)
    kwargs = json.loads(kwargs_json)

    # Import module dinamically
    mod = importlib.import_module(module_path)
    func = getattr(mod, function_name)

    func(*args, **kwargs)
