package transport

import (
	"context"
	"fmt"
	"net"
)

// startListener 建立本机 TCP 监听器，并把监听器所有权交给调用方。
func startListener(cfg ListenerConfig) (net.Listener, error) {
	// net.Listen 在指定地址上开始监听 TCP 端口，返回一个可以 Accept 连接的监听器。
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		// 常见失败：端口被占用、地址格式非法、权限不足等。
		return nil, fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	return listener, nil
}

// acceptConnection 阻塞接收一条入站连接。
func acceptConnection(ctx context.Context, listener net.Listener) (net.Conn, error) {
	// Accept 阻塞等待，直到有一条连接进来；监听器被关闭时也会返回错误。
	conn, err := listener.Accept()
	if err != nil {
		// 出错时先判断是不是因为上下文被取消（主程序主动退出）。
		if ctx != nil {
			// select 带 default 是“非阻塞检查”：Done 通道已关闭就走上面的 case，否则立刻走 default 不等待。
			select {
			case <-ctx.Done():
				// 是主动取消，返回上下文的错误（如 context.Canceled），让上层识别为正常退出。
				return nil, ctx.Err()
			default:
			}
		}
		// 否则是真正的接收错误。
		return nil, fmt.Errorf("accept connection: %w", err)
	}
	return conn, nil
}

// dialConnection 发起一次出站 TCP 连接尝试。
func dialConnection(ctx context.Context, cfg DialConfig) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// net.Dialer 是可配置的拨号器；Timeout 设定单次连接的最长等待时间，超时即失败。
	var dialer net.Dialer
	dialer.Timeout = cfg.DialTimeout

	// DialContext 发起 TCP 连接，同时受 ctx 控制：ctx 被取消会提前中断拨号。
	conn, err := dialer.DialContext(ctx, "tcp", cfg.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.PeerAddr, err)
	}
	return conn, nil
}

// closeConnection 关闭调用方持有的连接。
func closeConnection(conn net.Conn) error {
	// 关闭底层连接，释放系统资源（文件描述符/端口）。重复关闭同一连接会返回错误。
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}
	return nil
}
