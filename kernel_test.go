package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestRunCommandLineListsConnectionsAndQuits(t *testing.T) {
	var input strings.Reader
	input.Reset("list connections\n/quit\n")

	var output bytes.Buffer
	var cfg CommandLineConfig
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Input = &input
	cfg.Output = &output

	err := RunCommandLine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunCommandLine() error = %v", err)
	}

	text := output.String()
	if !strings.Contains(text, "listening on ") {
		t.Fatalf("output missing listen line: %q", text)
	}
}

func TestRunCommandLineReachReportsInvalidAddressAndContinues(t *testing.T) {
	var input strings.Reader
	input.Reset("reach bad-address\n/quit\n")

	var output bytes.Buffer
	var cfg CommandLineConfig
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Input = &input
	cfg.Output = &output

	err := RunCommandLine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunCommandLine() error = %v", err)
	}

	text := output.String()
	if !strings.Contains(text, "reach failed: invalid peer address") {
		t.Fatalf("output missing reach error: %q", text)
	}
}

func TestRunCommandLineReachAddsConnectionToList(t *testing.T) {
	peer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer peer.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := peer.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	var input strings.Reader
	input.Reset(fmt.Sprintf("reach %s\nlist connections\n/quit\n", peer.Addr().String()))

	var output bytes.Buffer
	var cfg CommandLineConfig
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.Input = &input
	cfg.Output = &output
	cfg.DialTimeout = time.Second

	err = RunCommandLine(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunCommandLine() error = %v", err)
	}

	// 对端的 Accept 协程与 RunCommandLine 的拨号是并发的，返回后连接不一定已入队；
	// 用带超时的等待代替 default，避免竞态导致的偶发失败。
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		t.Fatalf("peer did not accept reach connection")
	}

	text := output.String()
	idPattern := regexp.MustCompile(`(?m)^> [0-9]{8}$`)
	if !idPattern.MatchString(text) {
		t.Fatalf("output missing connection id: %q", text)
	}

	rowPattern := regexp.MustCompile(`\[[0-9]{8}\]\[localhost\]\[127\.0\.0\.1:[0-9]+\]`)
	if !rowPattern.MatchString(text) {
		t.Fatalf("output missing connection row: %q", text)
	}
}
