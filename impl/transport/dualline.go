package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

// closeLines 关闭连接建立阶段已经拿到的半成品连接。
func closeLines(lines Lines) {
	if lines.Inbound != nil {
		_ = lines.Inbound.Close()
	}
	if lines.Outbound != nil {
		_ = lines.Outbound.Close()
	}
}

// connectDualLine 同时启动监听和拨号流程，直到收发两条连接都建立完成。
// 返回后连接的所有权交给调用方，建立过程中产生的半连接会在失败时关闭。
func connectDualLine(ctx context.Context, cfg Config) (Lines, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// 双线连接：一条用于“收”，一条用于“发”。这里先把本机监听端口起起来负责收。
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return Lines{}, fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	defer listener.Close()

	// 派生一个可取消的子上下文：一旦两条线都连好（或出错），cancel 会通知两个后台协程停下。
	setupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 带缓冲(容量1)的通道：即便主协程还没来得及接收，后台协程也能先把结果放进去不阻塞。
	// 通道(channel)是 goroutine 之间传递数据的管道，天然线程安全。
	inboundCh := make(chan net.Conn, 1)  // 收线连接结果
	acceptErrCh := make(chan error, 1)   // 接收过程的错误
	outboundCh := make(chan net.Conn, 1) // 发线连接结果

	// 后台协程1：只接收一条入站连接（收线）。
	log.Printf("receiving on %s", cfg.ListenAddr)
	go acceptOne(setupCtx, listener, inboundCh, acceptErrCh)

	// 后台协程2：不停向对端拨号直到连上（发线）。
	log.Printf("sending to %s", cfg.PeerAddr)
	go dialUntilConnected(setupCtx, cfg.PeerAddr, cfg.RetryInterval, outboundCh)

	// 循环条件：只要收线或发线还有一条没连上，就继续等。
	var lines Lines
	for lines.Inbound == nil || lines.Outbound == nil {
		// select 会阻塞等待多个通道中“任意一个”就绪，谁先来就处理谁。
		select {
		case conn := <-inboundCh:
			// 收线连上了。
			lines.Inbound = conn
			log.Printf("receive line connected from %s", conn.RemoteAddr())
		case conn := <-outboundCh:
			// 发线连上了。
			lines.Outbound = conn
			log.Printf("send line connected to %s", conn.RemoteAddr())
		case err := <-acceptErrCh:
			// 接收出错：把已经连上的半成品连接关掉，整体失败返回。
			closeLines(lines)
			return Lines{}, err
		case <-setupCtx.Done():
			// 上下文被取消：同样清理半连接并返回取消错误。
			closeLines(lines)
			return Lines{}, setupCtx.Err()
		}
	}

	// 两条线都就绪，主动 cancel 让还在运行的后台协程尽快退出，然后把连接交给调用方。
	cancel()
	return lines, nil
}

// acceptOne 只接收一个入站连接，并通过通道把连接或监听错误交回协调协程。
func acceptOne(ctx context.Context, listener net.Listener, connCh chan<- net.Conn, errCh chan<- error) {
	conn, err := listener.Accept()
	if err != nil {
		// 出错时二选一：如果上下文已取消就直接退出（不必上报），
		// 否则尝试把错误发进 errCh。用 select 是为了在协程即将退出时不会永远卡在发送上。
		select {
		case <-ctx.Done():
			return
		case errCh <- fmt.Errorf("accept connection: %w", err):
			return
		}
	}

	// 成功拿到连接，尝试送回给协调协程；
	// 若此时上下文已取消（对方不再接收），就把这条连接关掉避免泄漏。
	select {
	case connCh <- conn:
	case <-ctx.Done():
		_ = conn.Close()
	}
}

// dialUntilConnected 按项目配置的间隔持续拨号，直到成功或上下文取消。
func dialUntilConnected(ctx context.Context, addr string, retryInterval time.Duration, connCh chan<- net.Conn) {
	for {
		// 每轮尝试拨号，最多等 3 秒。
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			// 连上了，把连接送回去（同样在取消时负责关闭），然后结束协程。
			select {
			case connCh <- conn:
			case <-ctx.Done():
				_ = conn.Close()
			}
			return
		}

		// 连接失败：记录日志，准备等待 retryInterval 后重试。
		log.Printf("connect to %s failed: %v; retrying in %s", addr, err, retryInterval)
		// Timer 在 retryInterval 之后往 timer.C 通道发一个信号，实现“定时等待”。
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			// 等待期间被取消：停掉定时器（释放资源）并退出，不再重试。
			timer.Stop()
			return
		case <-timer.C:
			// 定时到了，进入下一轮循环重试。
		}
	}
}
