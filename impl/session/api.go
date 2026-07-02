package session

import (
	"errors"
	"io"
	"net"
	"strings"
)

const defaultPrompt = "> "

// Config 是 session 模块暴露给主程序的收发配置。
type Config struct {
	Inbound  net.Conn
	Outbound net.Conn
	Input    io.Reader
	Output   io.Writer
	Prompt   string
}

// withDefaults 补齐 session 配置中可由项目默认值代替的输入输出对象。
func (cfg Config) withDefaults() Config {
	if cfg.Input == nil {
		cfg.Input = strings.NewReader("")
	}
	if cfg.Output == nil {
		cfg.Output = io.Discard
	}
	if cfg.Prompt == "" {
		cfg.Prompt = defaultPrompt
	}
	return cfg
}

// validate 确认会话收发必须依赖的两条连接已经建立。
func (cfg Config) validate() error {
	if cfg.Inbound == nil {
		return errors.New("missing inbound connection")
	}
	if cfg.Outbound == nil {
		return errors.New("missing outbound connection")
	}
	return nil
}

// Handle 是 session 模块暴露给主程序的会话 API。
// 它负责校验连接并补齐输入输出对象，内部收发实现只处理整理好的配置。
func Handle(cfg Config) error {
	// 输入提醒：
	// - Config: session.Config，会话收发配置。
	//   - Inbound: 接收对端消息的连接。
	//   - Outbound: 向对端发送消息的连接。
	//   - Input: 本地终端或测试替身使用的输入对象；为 nil 时使用空输入。
	//   - Output: 本地终端或测试替身使用的输出对象；为 nil 时丢弃输出。
	//   - Prompt: 本地输入提示符，为空时使用默认提示符。
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	return handleSession(cfg)
}
