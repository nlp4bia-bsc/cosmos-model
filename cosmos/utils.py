# utils.py
import os
import paramiko
import sys

from typing import Dict, List, Optional


def tail_file(
    ssh_client: paramiko.SSHClient,
    remote_path: str,
    last_position: int,
    filename: Optional[str] = None
) -> int:
    """
    Reads from a specific position to the end of a remote file.

    This function simulates the behavior of the `tail` command by reading data
    from a given position in a remote file to the end. The output is streamed
    to the local console.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    remote_path : str
        The path to the remote file.
    last_position : int
        The position (in bytes) from where to start reading the file.
    filename : Optional[str], optional
        The name of the file, used for tagging log output. Defaults to None.

    Returns:
    -------
    int
        The new position (in bytes) after reading to the end of the file.

    Notes:
    ------
    - If the file does not exist or cannot be accessed, the function returns the
      original `last_position`.
    - Any new data read from the file is streamed to the local console with an
      optional tag indicating the file name.
    """
    try:
        sftp = ssh_client.open_sftp()
        with sftp.open(remote_path, 'r') as f:
            f.seek(last_position)
            data = f.read().decode('utf-8')
            if data:
                sys.stdout.write(f"[job.{filename}] {data}\n")
                sys.stdout.flush()
            new_pos = f.tell()
        sftp.close()
        return new_pos
    except IOError:
        # File it can not exist
        return last_position


def load_template(template_filename: str) -> str:
    """
    Loads a template file from the "templates" directory and returns its content as a string.

    This function searches for the specified template file in the "templates" subdirectory
    relative to the current script's directory. It reads the file and returns its content.

    Parameters:
    ----------
    template_filename : str
        The name of the template file to load.

    Returns:
    -------
    str
        The content of the template file as a string.

    Raises:
    -------
    FileNotFoundError
        If the template file does not exist.
    IOError
        If there is an issue reading the file.

    Notes:
    ------
    - The function assumes the "templates" directory exists in the same directory as the script.
    """
    current_dir = os.path.dirname(__file__)
    template_path = os.path.join(current_dir, "templates", template_filename)
    with open(template_path, 'r') as f:
        content = f.read()

    return content


def create_local_entry_script() -> str:
    """
    Creates a temporary Python script based on the "entry_script.py" template.

    This function loads the content of the `entry_script.py` template located in the
    "templates" directory, writes it to a temporary Python file, and returns the path
    to the created file.

    Returns:
    -------
    str
        The file path of the created temporary Python script.

    Raises:
    -------
    FileNotFoundError
        If the "entry_script.py" template file does not exist.
    IOError
        If there is an issue reading or writing the file.

    Notes:
    ------
    - The function creates a temporary file using Python's `tempfile` module.
    - The temporary file is not automatically deleted upon script termination
      (set by `delete=False`), so it must be managed manually.
    """
    import tempfile
    import os

    template_path = os.path.join(
        os.path.dirname(__file__),
        "templates",
        "entry_script.py"
    )
    with open(template_path, "r") as f:
        content = f.read()

    temp_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
    temp_file_path = temp_file.name
    temp_file.write(content.encode("utf-8"))
    temp_file.close()

    return temp_file_path


def parse_pip_freeze(freeze_output: str) -> Dict[str, str]:
    """
    Parses the output of `pip freeze` and returns a dictionary of
    installed packages with their versions.

    This function processes the output of `pip freeze`, extracting the package names and their
    respective versions, and returns them in a dictionary format.

    Parameters:
    ----------
    freeze_output : str
        The raw string output from the `pip freeze` command.

    Returns:
    -------
    Dict[str, str]
        A dictionary where the keys are package names (lowercased)
        and the values are their versions.

    Notes:
    ------
    - Only packages with explicit version specifications (i.e., `package==version`) are included.
    - Packages without a version or other formats (e.g., editable installs) are ignored.
    """
    lines = freeze_output.strip().splitlines()
    installed = {}
    for line in lines:
        if "==" in line:
            pkg, ver = line.split("==", 1)
            installed[pkg.lower()] = ver
        else:
            pass

    return installed


def compute_missing_packages(
    requirements: List[str],
    installed_packages: Dict[str, str]
) -> List[str]:
    """
    Computes the list of packages that need to be installed or updated.

    This function compares a list of required packages and their versions (`requirements`)
    against the currently installed packages (`installed_packages`). It returns a list of
    packages that are missing or require a version update.

    Parameters:
    ----------
    requirements : List[str]
        A list of required packages with optional version specifications (e.g., `package==version`).
    installed_packages : Dict[str, str]
        A dictionary of currently installed packages and their versions. The keys are package
        names (lowercased), and the values are version strings.

    Returns:
    -------
    List[str]
        A list of packages from `requirements` that are missing or have a mismatched version.

    Notes:
    ------
    - Packages without version specifications in `requirements` are checked only for existence.
    - If a package version in `installed_packages` does not match the specified version in
      `requirements`, it will be included in the result.
    """
    missing = []
    for req in requirements:
        if "==" in req:
            pkg, ver = req.split("==", 1)
            pkg_lower = pkg.lower()

            if pkg_lower not in installed_packages:
                missing.append(req)

            else:
                installed_ver = installed_packages[pkg_lower]
                if installed_ver != ver:
                    missing.append(req)

        else:
            pkg_lower = req.lower()
            if pkg_lower not in installed_packages:
                missing.append(req)

    return missing
