# MuonMesh TCP Kernel

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个小型 Go TCP 连接管理程序。每个实例会启动一个本机监听器，然后进入交互式命令行。命令行是程序内核：它接收用户命令，管理连接 ID，并通过根 API 调用传输模块。

## 运行

启动一个实例：

```powershell
go run . -listen 127.0.0.1:9000
```

如果需要一个可连接的对端，可以在另一个终端启动第二个实例：

```powershell
go run . -listen 127.0.0.1:9001
```

## 命令

连接到其他节点，只尝试一次：

```text
reach <ip:port>
```

连接成功后，程序会输出一个随机 8 位连接 ID。连接失败时，命令会输出错误并结束本次尝试，不会自动重试。

按 ID 关闭连接：

```text
close <id>
```

列出当前连接：

```text
list connections
```

每一行的格式是：

```text
[id][连接发起人][连接接收人]
```

如果某一方是本机，则显示为 `localhost`；不是本机则显示为 `ip:port`。

退出程序：

```text
/quit
```

退出前，程序会先关闭监听器和所有活动连接。
