# Litebase Server

Litebase Server is a lightweight remote database management server that runs standalone or in a cluster, supporting read scaling with replicas and SQLite over HTTP.

## Overview

The code for the Litebase server is located in the`./pkg/server` directory of the repository.

## Configuration

Litebase Server can be configured to run from a global configuration file or in association with a specific project directory, allowing for flexible deployment scenarios using environment variables, command-line flags, or configuration files. When Litebase Server starts, it will look for a configuration file in the following order:

1. A global configuration file located at `~/.litebase/config.yaml` on Unix-like systems or `C:\ProgramData\litebase\config.yaml` on Windows.
2. A project-specific configuration file located at `./.litebase/config.yaml` within the current working directory.
3. User provided configuration via CLI flags.

If no configuration file is found, Litebase Server will use default settings.

## Starting the Server

The server can be started using the `litebase start` command. By default, the server will run in blocking mode, meaning it will occupy the terminal session until it is stopped. To run the server in the background, a process manager like `systemd` or `supervisord` can be used.

## Stopping the Server

Litebase Server can be stopped by sending a termination signal to the process. This can typically be done from your terminal by typing `Ctrl + C`.
