# initialization.py
from typing import Dict, Optional
from paramiko import SSHClient  # type: ignore

from cosmos.config import get_server_env_vars, load_dotenv_if_exists
from cosmos.ssh_connection import check_server_availability, create_ssh_client, remote_command


_global_cosmos_config: Dict = {}
_global_ssh_client: Optional[SSHClient] = None


def initialization(
    host: str,
    remote_base_path: Optional[str] = "/gpfs/projects/bsc14/executions",
) -> None:
    """
    Initializes the Cosmos system by performing the following steps:

    1. Loads the environment variables if a .env file exists.
    2. Verifies the SSH connection to the remote server using the provided credentials.
    3. Checks and ensures the existence of the remote folder for job execution.

    Parameters:
    ----------
    host: str
        The host where the library will execute the job.
    remote_base_path: str
        Path to remote folder in remote server to save all the execution details
        of the Job.

    Raises:
    -------
    ConnectionError
        If the server is unavailable or the SSH connection fails.
    ValueError
        If the `remote_base_path` is not defined in the configuration file.

    Returns:
    -------
    None
        This function does not return any value.
    """
    global _global_cosmos_config, _global_ssh_client

    # 1. Load environment variables if a .env file exists.
    load_dotenv_if_exists()

    # 2. Read the Cosmos configuration.
    if host:
        _global_cosmos_config['host'] = host

    if remote_base_path:
        _global_cosmos_config['remote_base_path'] = remote_base_path

    # 3. Verify the SSH connection to the server.
    server_env = get_server_env_vars()
    print(f"[cosmos.initialization] Creating SSH connection to {_global_cosmos_config['host']}")
    ssh_client = create_ssh_client(
        host=_global_cosmos_config["host"],
        port=server_env["port"],
        user=server_env["user"],
        password=server_env["password"],
        key_filename=server_env["key_filename"]
    )

    # Check server availability and close the connection if it fails.
    if not check_server_availability(ssh_client):
        ssh_client.close()
        raise ConnectionError("Server not available or ping verification failed")

    # 4. Verify or create the remote folder for job execution.
    remote_base_path = _global_cosmos_config.get("remote_base_path")
    check_or_create_remote_path(ssh_client, remote_base_path)

    # Store the SSH client in a global variable for further use.
    _global_ssh_client = ssh_client

    print(
        "[cosmos.initialization] Connection established and remote path '"
        f"{remote_base_path}' verified."
    )


def check_or_create_remote_path(ssh_client: SSHClient, remote_path: str) -> None:
    """
    Checks if a remote path exists on the server and creates it if it does not.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    remote_path : str
        The remote directory path to check or create.

    Raises:
    -------
    Exception
        If an unexpected error occurs while verifying or creating the remote path.

    Notes:
    ------
    This function uses the `mkdir -p` command to ensure the directory is created
    without raising an error if it already exists.
    """
    # Command to create the directory if it does not exist
    cmd = f"mkdir -p {remote_path} && chmod -R 777 {remote_path}"

    # Execute the remote command and capture any error output
    _, err = remote_command(ssh_client, cmd)

    # Log an error message if there is an issue, except for the expected "bsc/1.0" warning
    if err.strip() and "bsc/1.0" not in err:
        print(f"[ERROR] It could not verify or create the remote path {remote_path}: {err}")


def get_global_config() -> Dict:
    """
    Retrieves the global Cosmos configuration.

    This function returns the global configuration object for the Cosmos system,
    which is loaded during the initialization process.

    Returns:
    -------
    dict
        The global configuration dictionary for Cosmos.
    """
    return _global_cosmos_config


def get_global_ssh_client() -> Optional[SSHClient]:
    """
    Retrieves the global SSH client instance.

    This function returns the global SSH client object, which is established
    during the initialization process for connecting to the remote server.

    Returns:
    -------
    Optional[paramiko.SSHClient]
        The global SSH client instance for interacting with the remote server.
        If no SSH client has been established, it returns None.
    """
    return _global_ssh_client
