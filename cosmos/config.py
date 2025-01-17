# config.py
import os
import yaml

from typing import Dict, Optional

from dotenv import load_dotenv

DEFAULT_COSMOS_CONFIG_NAME = "cosmos_config.yml"


def load_dotenv_if_exists() -> None:
    """
    Loads environment variables from a .env file if it exists.

    This function attempts to load a `.env` file located in the current working
    directory (CWD) or a default path recognized by the `load_dotenv` method.
    If no `.env` file is found, the function will silently continue without errors.

    Returns:
    -------
    None
        This function does not return any value.
    """
    # Attempt to load environment variables from a .env file.
    # If no file is found, the function will silently continue without errors.
    load_dotenv()


def read_cosmos_config(config_path: str = None):
    """
    Reads the Cosmos configuration file (cosmos_config.yml)
    from a specified path or a default location.

    Parameters:
    ----------
    config_path : str, optional
        The path to the configuration file. If not provided, the function will look
        for the default configuration file in the current working directory.

    Returns:
    -------
    dict
        A dictionary containing the loaded configuration data.

    Raises:
    -------
    FileNotFoundError
        If the configuration file does not exist at the specified or default path.
    """
    # If no configuration path is provided, use the default path in the current working directory.
    if not config_path:
        config_path = os.path.join(os.getcwd(), DEFAULT_COSMOS_CONFIG_NAME)

    # Check if the configuration file exists; raise an error if it does not.
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file does not exist: {config_path}")

    # Open the configuration file and parse its contents as a dictionary.
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)

    return cfg


def get_server_env_vars() -> Dict[str, Optional[str]]:
    """
    Retrieves environment variables needed to connect to a remote server.

    This function processes the credentials and connection details for a remote server
    by reading specific environment variables.

    Returns:
    -------
    dict
        A dictionary containing the following keys:
        - "port" : int
            The SSH port to connect to (default is 22).
        - "user" : str or None
            The username for SSH authentication.
        - "password" : str or None
            The password for SSH authentication.
        - "key_filename" : str or None
            The path to the private key file for SSH authentication.
    """
    # Retrieve SSH connection details from environment variables.
    ssh_user: Optional[str] = os.getenv("COSMOS_SSH_USER")  # SSH username
    ssh_password: Optional[str] = os.getenv("COSMOS_SSH_PASSWORD")  # SSH password
    ssh_keyfile: Optional[str] = os.getenv("COSMOS_SSH_KEYFILE")  # Path to SSH private key file
    ssh_port: int = int(os.getenv("COSMOS_SSH_PORT", 22))  # SSH port, defaults to 22

    # Return the connection details as a dictionary.
    return {
        "port": ssh_port,
        "user": ssh_user,
        "password": ssh_password,
        "key_filename": ssh_keyfile
    }
