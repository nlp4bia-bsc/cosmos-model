import sys
import time
import paramiko

from typing import Optional, Tuple


def create_ssh_client(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    key_filename: Optional[str] = None
) -> paramiko.SSHClient:
    """
    Creates an SSH connection to a remote server.

    This function establishes a secure SSH connection to the specified remote server
    using the provided credentials or key file.

    Parameters:
    ----------
    host : str
        The hostname or IP address of the remote server.
    port : int
        The port to use for the SSH connection.
    user : str
        The username for authentication.
    password : Optional[str], optional
        The password for authentication. Defaults to None.
    key_filename : Optional[str], optional
        The path to the private key file for authentication. Defaults to None.

    Returns:
    -------
    paramiko.SSHClient
        An active `paramiko.SSHClient` instance connected to the remote server.

    Notes:
    ------
    - If both `password` and `key_filename` are provided, `paramiko` will prioritize
      the private key file for authentication.
    - The function uses `AutoAddPolicy` to automatically add the server's host key
      to the known hosts list. This may not be suitable for highly secure environments.
    """

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        key_filename=key_filename
    )

    return ssh


def remote_command(ssh_client: paramiko.SSHClient, command: str) -> Tuple[str, str]:
    """
    Executes a remote command on a server via SSH and returns the output.

    This function runs the specified command on the remote server using the provided
    SSH client and returns the standard output and standard error as strings. It filters
    out lines containing the string "bsc/1.0".

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    command : str
        The command to execute on the remote server.

    Returns:
    -------
    Tuple[str, str]
        A tuple containing:
        - stdout: The standard output of the command, filtered.
        - stderr: The standard error of the command, filtered.

    Notes:
    ------
    - The function filters out lines containing "bsc/1.0" from both stdout and stderr.
    - The returned strings are decoded using UTF-8.
    """
    stdin, stdout, stderr = ssh_client.exec_command(command)
    out = stdout.read().decode('utf-8')
    err = stderr.read().decode('utf-8')
    filtered_stdout = [line for line in out if "bsc/1.0" not in line]
    filtered_stderr = [line for line in err if "bsc/1.0" not in line]

    return "".join(filtered_stdout), "".join(filtered_stderr)


def scp_file(ssh_client: paramiko.SSHClient, local_path: str, remote_path: str) -> None:
    """
    Copies a local file to a remote server using SFTP.

    This function uses the SFTP functionality of the provided SSH client to transfer
    a file from the local system to a specified path on the remote server.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    local_path : str
        The full path to the file on the local system to be copied.
    remote_path : str
        The destination path on the remote server where the file should be copied.

    Returns:
    -------
    None
        This function does not return any value.

    Notes:
    ------
    - If the `remote_path` directory does not exist, the transfer will fail.
    - The function opens an SFTP session using the provided SSH client and closes it
      automatically when the transfer is complete.
    """
    with ssh_client.open_sftp() as sftp:
        sftp.put(local_path, remote_path)


def check_server_availability(ssh_client: paramiko.SSHClient) -> bool:
    """
    Checks the availability of a server by executing a basic ping command.

    This function attempts to execute a simple command on the remote server to verify
    its availability. If the command executes successfully and returns the expected
    output, the server is considered available.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.

    Returns:
    -------
    bool
        True if the server is available (command executed successfully and returned
        the expected output), otherwise False.

    Notes:
    ------
    - The function uses a basic echo command to check connectivity.
    - Any exception during the command execution is caught and treated as an
      indication that the server is unavailable.
    """
    try:
        out, err = remote_command(ssh_client, "echo 'ping_ok'")
        return "ping_ok" in out
    except:  # noqa: E722
        return False


def remote_command_stream(ssh_client: paramiko.SSHClient, command: str) -> Optional[int]:
    """
    Executes a command on a remote server and streams its output in real time.

    This function runs the specified command on the remote server using the provided
    SSH client. The standard output and standard error are streamed in real time to
    the local console.

    Parameters:
    ----------
    ssh_client : paramiko.SSHClient
        An active SSH client connected to the remote server.
    command : str
        The command to execute on the remote server.

    Returns:
    -------
    Optional[int]
        The exit code of the command if available, otherwise None.

    Notes:
    ------
    - The function uses Paramiko's low-level session channel for real-time output
      streaming.
    - Both stdout and stderr are streamed to the local console in real time.
    - The function waits for the command to complete and ensures that any remaining
      data in the buffers is processed before closing the channel.
    """

    transport = ssh_client.get_transport()
    channel = transport.open_session()

    channel.get_pty()
    channel.exec_command(command)

    exit_code = None

    while True:
        if channel.exit_status_ready():
            exit_code = channel.recv_exit_status()
            break

        if channel.recv_ready():
            out_data = channel.recv(1024).decode("utf-8", errors="replace")
            sys.stdout.write(out_data)
            sys.stdout.flush()

        if channel.recv_stderr_ready():
            err_data = channel.recv_stderr(1024).decode("utf-8", errors="replace")
            sys.stdout.write(err_data)
            sys.stdout.flush()
            time.sleep(0.1)

    while channel.recv_ready():
        out_data = channel.recv(1024).decode("utf-8", errors="replace")
        sys.stdout.write(out_data)
        sys.stdout.flush()

    while channel.recv_stderr_ready():
        err_data = channel.recv_stderr(1024).decode("utf-8", errors="replace")
        sys.stdout.write(err_data)
        sys.stdout.flush()

    channel.close()
    return exit_code
