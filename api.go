package main

import (
	"context"
	"net"

	"neutrino/impl/transport"
)

// ListenerConfig 是主程序使用的监听配置别名。
type ListenerConfig = transport.ListenerConfig

// DialConfig 是主程序使用的连接配置别名。
type DialConfig = transport.DialConfig

// Connection 是主程序使用的连接别名。
type Connection = net.Conn

// Listener 是主程序使用的监听器别名。
type Listener = net.Listener

// StartListener 启动主程序需要的本机监听。
func StartListener(cfg ListenerConfig) (Listener, error) {
	return transport.StartListener(cfg)
}

// AcceptConnection 接收主程序需要的入站连接。
func AcceptConnection(ctx context.Context, listener Listener) (Connection, error) {
	return transport.AcceptConnection(ctx, listener)
}

// DialConnection 建立主程序需要的出站连接。
func DialConnection(ctx context.Context, cfg DialConfig) (Connection, error) {
	return transport.DialConnection(ctx, cfg)
}

// CloseConnection 关闭主程序持有的连接。
func CloseConnection(conn Connection) error {
	return transport.CloseConnection(conn)
}
