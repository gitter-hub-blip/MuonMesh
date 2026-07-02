package session

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

// lockedWriter 串行化终端输出，避免接收消息和输入提示符互相覆盖。
type lockedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

// isClosedNetworkError 判断错误是否来自连接被协程协同关闭。
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// 当我们主动关闭连接时，另一个正在读/写的协程会拿到这类错误。
	// 这属于“预期内的正常关闭”，靠匹配错误信息文本来识别，从而不当成真正的故障上报。
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "An existing connection was forcibly closed")
}

// Print 在持有输出锁时写入普通文本。
func (w *lockedWriter) Print(args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = fmt.Fprint(w.writer, args...)
}

// Printf 在持有输出锁时写入格式化文本。
func (w *lockedWriter) Printf(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = fmt.Fprintf(w.writer, format, args...)
}

// handleSession 启动接收和发送两个循环，并在任一方向结束时统一关闭连接。
func handleSession(cfg Config) error {
	// defer 在函数返回时才执行，保证两条连接最终一定被关闭（兜底清理）。
	defer cfg.Inbound.Close()
	defer cfg.Outbound.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 容量为 2 的错误通道：收、发两个协程各自可能上报一次，缓冲 2 保证它们发送时不会被阻塞住。
	errCh := make(chan error, 2)
	// sync.Once 保证包裹的函数只会真正执行一次，即使被多个协程重复调用。
	var closeOnce sync.Once
	// closeConn 是一个闭包：无论谁先结束会话，都调用它统一取消上下文并关闭两条连接。
	// 用 Once 包住是因为收发两方可能几乎同时触发关闭，避免重复关闭。
	closeConn := func() {
		closeOnce.Do(func() {
			cancel()
			_ = cfg.Inbound.Close()
			_ = cfg.Outbound.Close()
		})
	}

	// 启动两个并发循环：一个专门收对端消息，一个专门读本地输入并发出去。
	output := &lockedWriter{writer: cfg.Output}
	go receiveLoop(ctx, cfg.Inbound, output, cfg.Prompt, errCh, closeConn)
	go sendLoop(ctx, cfg.Outbound, cfg.Input, output, cfg.Prompt, errCh, closeConn)

	// 阻塞等待第一个结束信号（任一方向出错或正常结束都会往 errCh 发一次）。
	err := <-errCh
	closeConn()

	// EOF（对端正常断开）和“主动关闭连接”都算正常收尾，不当作错误返回；其余才是真正的故障。
	if err != nil && !errors.Is(err, io.EOF) && !isClosedNetworkError(err) {
		return err
	}
	log.Println("lines closed")
	return nil
}

// receiveLoop 从接收连接读取对端消息，并用加锁输出避免和本地提示符交错。
func receiveLoop(ctx context.Context, conn net.Conn, output *lockedWriter, prompt string, errCh chan<- error, closeConn func()) {
	// bufio.NewReader 包装连接，让我们能方便地“按行”读取。
	reader := bufio.NewReader(conn)

	for {
		// 每轮开始先非阻塞地检查是否已被取消，是则退出循环，结束本协程。
		select {
		case <-ctx.Done():
			return
		default:
		}

		// ReadString('\n') 一直读到换行符为止，得到对端发来的一整行消息。
		message, err := reader.ReadString('\n')
		if err != nil {
			// 读失败（含对端断开 EOF）：上报错误并触发统一关闭。
			errCh <- fmt.Errorf("receive failed: %w", err)
			closeConn()
			return
		}

		// \r 回车把光标移到行首，先覆盖掉当前提示符行显示对端消息，末尾再补回提示符，界面更整齐。
		output.Printf("\rpeer: %s%s", message, prompt)
	}
}

// sendLoop 从本地输入读取消息并写入发送连接，/quit 会主动结束会话。
func sendLoop(ctx context.Context, conn net.Conn, input io.Reader, output *lockedWriter, prompt string, errCh chan<- error, closeConn func()) {
	// Scanner 按行读本地输入；Writer 带缓冲地往连接写数据。
	scanner := bufio.NewScanner(input)
	writer := bufio.NewWriter(conn)

	// 先打印一次提示符，等待用户输入。
	output.Print(prompt)
	// scanner.Scan() 每成功读到一行就返回 true，进入循环体。
	for scanner.Scan() {
		// 同样先检查是否已被取消。
		select {
		case <-ctx.Done():
			return
		default:
		}

		text := scanner.Text()
		// EqualFold 做不区分大小写的比较，所以 /quit、/QUIT 都能触发退出。
		if strings.EqualFold(strings.TrimSpace(text), "/quit") {
			// 用户主动退出：往 errCh 发 nil 表示“正常结束”，再统一关闭。
			errCh <- nil
			closeConn()
			return
		}

		// 把这行文本加上换行写进缓冲区。返回值第一个是写入字节数，这里用 _ 忽略。
		if _, err := writer.WriteString(text + "\n"); err != nil {
			errCh <- fmt.Errorf("send failed: %w", err)
			closeConn()
			return
		}
		// 缓冲写入必须 Flush 才会真正发送到网络，否则数据可能滞留在缓冲区里发不出去。
		if err := writer.Flush(); err != nil {
			errCh <- fmt.Errorf("flush failed: %w", err)
			closeConn()
			return
		}

		// 一行发送完毕，重新打印提示符等待下一行。
		output.Print(prompt)
	}

	// 循环因输入结束(EOF)而退出：区分是读取出错还是正常结束，分别上报。
	if err := scanner.Err(); err != nil {
		errCh <- fmt.Errorf("read input failed: %w", err)
	} else {
		errCh <- nil
	}
	closeConn()
}
