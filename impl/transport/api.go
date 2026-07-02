package transport

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// Config 是 transport 模块暴露给主程序的连接配置。
type Config struct {
	ListenAddr    string
	PeerAddr      string
	RetryInterval time.Duration
}

// Lines 是 transport 模块返回给主程序的双线连接结果。
// Inbound 用于接收对端消息，Outbound 用于向对端发送消息。
type Lines struct {
	Inbound  net.Conn
	Outbound net.Conn
}

// ListenerConfig 是 transport 模块暴露给主程序的监听配置。
type ListenerConfig struct {
	ListenAddr string
}

// DialConfig 是 transport 模块暴露给主程序的出站连接配置。
type DialConfig struct {
	PeerAddr    string
	DialTimeout time.Duration
}

// withDefaults 补齐 transport 配置中由项目约定提供的默认值。
func (cfg Config) withDefaults() Config {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = time.Second
	}
	return cfg
}

// validate 在进入连接实现前检查地址配置，避免内部协程处理无效输入。
func (cfg Config) validate() error {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return fmt.Errorf("listen address is empty")
	}
	if strings.TrimSpace(cfg.PeerAddr) == "" {
		return fmt.Errorf("peer address is empty")
	}
	if cfg.RetryInterval < 0 {
		return fmt.Errorf("retry interval is invalid")
	}
	if _, err := net.ResolveTCPAddr("tcp", cfg.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen address %q: %w", cfg.ListenAddr, err)
	}
	if _, err := net.ResolveTCPAddr("tcp", cfg.PeerAddr); err != nil {
		return fmt.Errorf("invalid peer address %q: %w", cfg.PeerAddr, err)
	}
	return nil
}

// withDefaults 补齐出站连接配置中由项目约定提供的默认值。
func (cfg DialConfig) withDefaults() DialConfig {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	return cfg
}

// validate 在启动监听前检查本机监听地址。
func (cfg ListenerConfig) validate() error {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return fmt.Errorf("listen address is empty")
	}
	if _, err := net.ResolveTCPAddr("tcp", cfg.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen address %q: %w", cfg.ListenAddr, err)
	}
	return nil
}

// validate 在发起出站连接前检查对端地址和超时时间。
func (cfg DialConfig) validate() error {
	if strings.TrimSpace(cfg.PeerAddr) == "" {
		return fmt.Errorf("peer address is empty")
	}
	if cfg.DialTimeout < 0 {
		return fmt.Errorf("dial timeout is invalid")
	}
	if _, err := net.ResolveTCPAddr("tcp", cfg.PeerAddr); err != nil {
		return fmt.Errorf("invalid peer address %q: %w", cfg.PeerAddr, err)
	}
	return nil
}

// ConnectDualLine 是 transport 模块暴露给主程序的连接 API。
// 它负责校验输入，并在内部建立接收和发送两条连接。
func ConnectDualLine(ctx context.Context, cfg Config) (Lines, error) {
	// 输入提醒：
	// - context.Context: 标准库 context 包的上下文类型；用于控制连接建立流程的取消；为 nil 时内部会使用 context.Background()。
	// - Config: transport.Config，连接建立配置。
	//   - ListenAddr: 本地监听地址，用于接收入站连接。
	//   - PeerAddr: 对端地址，用于建立出站连接。
	//   - RetryInterval: 出站连接失败后的重试间隔；为 0 时使用项目默认值。
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return Lines{}, err
	}
	return connectDualLine(ctx, cfg)
}

// StartListener 是 transport 模块暴露给主程序的监听 API。
func StartListener(cfg ListenerConfig) (net.Listener, error) {
	// 输入提醒：
	// - ListenerConfig: transport.ListenerConfig，监听配置。
	//   - ListenAddr: 本机监听地址，用于接收入站连接。
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return startListener(cfg)
}

// AcceptConnection 是 transport 模块暴露给主程序的入站连接 API。
func AcceptConnection(ctx context.Context, listener net.Listener) (net.Conn, error) {
	// 输入提醒：
	// - context.Context: 标准库 context 包的上下文类型；用于识别监听关闭是否来自主程序取消。
	// - net.Listener: 标准库 net 包的监听器类型；由 StartListener 返回。
	if listener == nil {
		return nil, fmt.Errorf("listener is nil")
	}
	return acceptConnection(ctx, listener)
}

// DialConnection 是 transport 模块暴露给主程序的出站连接 API。
func DialConnection(ctx context.Context, cfg DialConfig) (net.Conn, error) {
	// 输入提醒：
	// - context.Context: 标准库 context 包的上下文类型；用于控制单次连接尝试的取消。
	// - DialConfig: transport.DialConfig，出站连接配置。
	//   - PeerAddr: 对端地址，用于建立出站连接。
	//   - DialTimeout: 单次连接超时时间；为 0 时使用项目默认值。
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return dialConnection(ctx, cfg)
}

// CloseConnection 是 transport 模块暴露给主程序的连接关闭 API。
func CloseConnection(conn net.Conn) error {
	// 输入提醒：
	// - net.Conn: 标准库 net 包的连接类型；由主程序持有并负责关闭。
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return closeConnection(conn)
}
