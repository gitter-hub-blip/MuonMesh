# Neutrinonet TCP Kernel

[English](README.md) | [简体中文](README.zh-CN.md)

This is a small Go TCP connection-management program. Each instance starts a
local listener and then opens an interactive command line. The command line is
the program kernel: it accepts user commands, manages connection IDs, and calls
the transport module through the root API.

## Run

Start one instance:

```powershell
go run . -listen 127.0.0.1:9000
```

Start another instance if you want a peer to connect to:

```powershell
go run . -listen 127.0.0.1:9001
```

## Commands

Connect to another node once:

```text
reach <ip:port>
```

If the connection succeeds, the program prints a random 8-digit connection ID.
If it fails, the command reports the error and does not retry.

Close a connection by ID:

```text
close <id>
```

List current connections:

```text
list connections
```

Rows are printed as:

```text
[id][initiator][receiver]
```

If either side is the local instance, it is printed as `localhost`; remote sides
are printed as `ip:port`.

Quit the program:

```text
/quit
```

Before exiting, the program closes the listener and all active connections.
