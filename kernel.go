package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	commandPrompt      = "> "
	defaultDialTimeout = 3 * time.Second
	localName          = "localhost"
)

// CommandLineConfig 是主程序命令行使用的配置。
type CommandLineConfig struct {
	ListenAddr  string
	Input       io.Reader
	Output      io.Writer
	DialTimeout time.Duration
}

type connectionItem struct {
	ID        string
	Initiator string
	Receiver  string
	conn      Connection
}

type connectionManager struct {
	mu    sync.Mutex
	items map[string]*connectionItem
}

type lockedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

/**************************************************************
*
接下来是主程序的helper函数和方法，主要是对命令行配置、连接管理器和锁定写入器的操作。
*
**************************************************************/

func (cfg CommandLineConfig) withDefaults() CommandLineConfig {
	// 注意：这里的接收者是值类型 (cfg CommandLineConfig)，不是指针。
	// 所以 cfg 是调用方传入配置的一份拷贝，下面的修改不会影响原对象，
	// 而是把补好默认值的新配置通过 return 返回。这是 Go 里“不可变配置”的常见写法。
	if cfg.Input == nil {
		// 没有指定输入源时，用一个空字符串 Reader 兜底，避免后续读取时对 nil 解引用。
		cfg.Input = strings.NewReader("")
	}
	if cfg.Output == nil {
		// io.Discard 是一个“黑洞”写入器：写进去的内容直接丢弃，保证输出调用不会 panic。
		cfg.Output = io.Discard
	}
	if cfg.DialTimeout == 0 {
		// Go 里数值类型的零值是 0，用它来判断“调用方没设置”，然后填入项目默认超时。
		cfg.DialTimeout = defaultDialTimeout
	}
	return cfg
}

func newConnectionManager() *connectionManager {
	var manager connectionManager
	// map 的零值是 nil，nil map 只能读不能写；这里必须先 make 初始化才能往里存连接。
	manager.items = make(map[string]*connectionItem)
	// 返回结构体的地址（指针）。Go 允许返回局部变量地址，编译器会自动把它分配到堆上，
	// 这样多处共享的是同一个 manager，锁和 map 才是同一份。
	return &manager
}

func (manager *connectionManager) addOutbound(conn Connection) (string, error) {
	// 出站连接：是本机主动去连别人，所以发起方(Initiator)是本机，接收方(Receiver)是对端地址。
	var item connectionItem
	item.Initiator = localName
	item.Receiver = conn.RemoteAddr().String()
	item.conn = conn
	return manager.add(item)
}

func (manager *connectionManager) addInbound(conn Connection) (string, error) {
	// 入站连接：是别人主动连进来的，方向正好相反——发起方是对端，接收方是本机。
	var item connectionItem
	item.Initiator = conn.RemoteAddr().String()
	item.Receiver = localName
	item.conn = conn
	return manager.add(item)
}

func (manager *connectionManager) add(item connectionItem) (string, error) {
	// Lock/Unlock 保护共享的 items map：多个 goroutine（命令行、accept 协程）可能同时增删，
	// 不加锁会数据竞争甚至崩溃。defer Unlock 保证无论从哪个 return 退出，锁都会被释放。
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// newIDLocked 名字里的 Locked 是约定：调用它时必须已经持有锁（此处已 Lock），它内部不再自己加锁。
	id, err := manager.newIDLocked()
	if err != nil {
		return "", err
	}

	item.ID = id
	// item 是传值进来的拷贝，取它的地址存进 map；因为 map 里存的是指针 *connectionItem。
	manager.items[id] = &item
	return id, nil
}

func (manager *connectionManager) close(id string) error {
	manager.mu.Lock()
	// map 取值的“逗号 ok”写法：ok 为 true 表示 id 存在，false 表示不存在。
	item, ok := manager.items[id]
	if ok {
		// 只有存在时才从 map 删除，避免误删。
		delete(manager.items, id)
	}
	// 这里没用 defer，而是手动 Unlock：因为下面的 CloseConnection 是较慢的网络关闭操作，
	// 尽早释放锁，别让别的 goroutine 白等。锁只用来保护 map，不需要覆盖网络 IO。
	manager.mu.Unlock()

	if !ok {
		return fmt.Errorf("connection %s not found", id)
	}
	// 真正关闭底层 TCP 连接。
	return CloseConnection(item.conn)
}

func (manager *connectionManager) closeAll() {
	manager.mu.Lock()
	// 先在锁内把所有连接“快照”到一个切片里，同时清空 map。
	// make([]T, 0, n) 创建长度 0、预留容量 n 的切片，能减少 append 时的扩容次数。
	items := make([]*connectionItem, 0, len(manager.items))
	for id, item := range manager.items {
		items = append(items, item)
		// 在 range 遍历同一个 map 时删除当前 key 是安全的。
		delete(manager.items, id)
	}
	manager.mu.Unlock()

	// 出了锁再逐个关闭连接：关闭是慢操作，放在锁外避免长时间占用锁。
	// _ = 表示“显式忽略返回的 error”，这里是关闭全部连接，单个失败也不需要中断。
	for _, item := range items {
		_ = CloseConnection(item.conn)
	}
}

func (manager *connectionManager) list() []connectionItem {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// 返回的是 []connectionItem（值），不是 []*connectionItem（指针）。
	// *item 做了一次解引用拷贝，这样调用方拿到的是独立副本，改动不会影响内部数据，也更线程安全。
	items := make([]connectionItem, 0, len(manager.items))
	for _, item := range manager.items {
		items = append(items, *item)
	}
	// map 遍历顺序是随机的，所以按 ID 排序让输出稳定。
	// sort.Slice 的第二个参数是“比较函数”：返回 i 是否应排在 j 前面（这里按 ID 字符串升序）。
	sort.Slice(items, func(i int, j int) bool {
		return items[i].ID < items[j].ID
	})
	return items
}

func (manager *connectionManager) newIDLocked() (string, error) {
	// 最多尝试 32 次生成一个不重复的 8 位 ID，避免极端情况下无限循环。
	for i := 0; i < 32; i++ {
		// crypto/rand 的 rand.Int 生成 [0, 90000000) 区间的安全随机数（比 math/rand 更难预测）。
		value, err := rand.Int(rand.Reader, big.NewInt(90000000))
		if err != nil {
			// %w 会把底层错误“包裹”进新错误，调用方可用 errors.Is/As 追溯原始错误。
			return "", fmt.Errorf("generate connection id: %w", err)
		}

		// 加上 10000000 后区间变成 [10000000, 99999999)，%08d 补零成固定 8 位字符串。
		id := fmt.Sprintf("%08d", value.Int64()+10000000)
		// 用“逗号 ok”只判断存在性，_ 忽略取到的值；不冲突就采用这个 ID。
		if _, ok := manager.items[id]; !ok {
			return id, nil
		}
	}
	// 32 次都撞车说明 ID 空间几乎占满，返回错误由上层处理。
	return "", fmt.Errorf("connection id space is busy")
}

// lockedWriter 用一把锁把多个 goroutine 的输出串行化，防止提示符和收到的消息交错混在一起。
func (w *lockedWriter) Print(args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = fmt.Fprint(w.writer, args...)
}

func (w *lockedWriter) Printf(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = fmt.Fprintf(w.writer, format, args...)
}

/**************************************************************
*
接下来是主程序命令行的主要逻辑，负责读取用户输入的命令并调用各模块的 API 来执行相应的操作。
*
**************************************************************/

// RunCommandLine 启动主程序命令行，并通过根 API 编排各模块能力。
func RunCommandLine(ctx context.Context, cfg CommandLineConfig) error {
	cfg = cfg.withDefaults()
	if ctx == nil {
		// 允许调用方传 nil，用 Background() 作为最顶层的空上下文兜底。
		ctx = context.Background()
	}

	// WithCancel 派生出一个可手动取消的子上下文；cancel() 被调用后，所有监听此 ctx 的 goroutine 都会收到停止信号。
	// defer cancel() 保证函数退出时一定触发取消，避免 goroutine 泄漏。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var listenerCfg ListenerConfig
	listenerCfg.ListenAddr = cfg.ListenAddr

	// 先把本机监听起起来；失败就直接返回，后面的流程都依赖它。
	listener, err := StartListener(listenerCfg)
	if err != nil {
		return err
	}
	defer listener.Close()

	// &lockedWriter{...} 是结构体字面量取地址，直接构造并拿到指针。
	output := &lockedWriter{writer: cfg.Output}
	manager := newConnectionManager()

	output.Printf("listening on %s\n", listener.Addr().String())
	// go 关键字启动一个新 goroutine（轻量级线程）在后台不停接收入站连接，
	// 主流程则继续去读用户命令，二者并发运行。
	go acceptLoop(ctx, listener, manager, output)

	// 阻塞在命令循环里处理用户输入，直到 /quit 或输入结束才返回。
	err = runCommandLoop(ctx, cancel, cfg, listener, manager, output)
	// 退出前统一关闭所有还开着的连接，做收尾清理。
	manager.closeAll()
	return err
}

func runCommandLoop(ctx context.Context, cancel context.CancelFunc, cfg CommandLineConfig, listener Listener, manager *connectionManager, output *lockedWriter) error {
	// bufio.Scanner 默认按行读取输入，用起来比手动处理字节方便。
	scanner := bufio.NewScanner(cfg.Input)
	// for {} 没有条件，是无限循环，靠内部的 break/return 退出。
	for {
		output.Print(commandPrompt)
		// Scan() 读下一行，返回 false 表示读到文件末尾(EOF)或出错，此时退出循环。
		if !scanner.Scan() {
			break
		}

		// Text() 取本行内容，TrimSpace 去掉首尾空白（包括换行符）。
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			// 空行跳过本次循环，continue 直接进入下一轮。
			continue
		}

		if line == "/quit" {
			// 用户主动退出：取消上下文通知后台协程停止，并关掉监听。
			cancel()
			_ = listener.Close()
			return nil
		}

		// 其余命令交给分发函数处理。
		handleCommand(ctx, cfg, manager, output, line)
	}

	// 循环因输入结束而退出，同样做取消和关闭收尾。
	cancel()
	_ = listener.Close()

	// Scan 结束后要检查 Err()：EOF 属于正常结束会返回 nil，其它错误才需要上报。
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read command: %w", err)
	}
	return nil
}

func handleCommand(ctx context.Context, cfg CommandLineConfig, manager *connectionManager, output *lockedWriter, line string) {
	// Fields 按空白切分字符串，自动忽略多余空格，得到 [命令, 参数1, 参数2...]。
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}

	// 用第一个词(fields[0])作为命令名分发到不同处理函数。Go 的 switch 默认不会“贯穿”到下一个 case。
	switch fields[0] {
	case "reach":
		handleReach(ctx, cfg, manager, output, fields)
	case "close":
		handleClose(manager, output, fields)
	case "list":
		handleList(manager, output, fields)
	default:
		output.Printf("unknown command: %s\n", fields[0])
	}
}

func acceptLoop(ctx context.Context, listener Listener, manager *connectionManager, output *lockedWriter) {
	for {
		// AcceptConnection 会阻塞等待，直到有人连进来或监听被关闭。
		conn, err := AcceptConnection(ctx, listener)
		if err != nil {
			// select + case <-ctx.Done() 用来区分错误原因：
			// 如果是因为上下文被取消（正常退出）导致的失败，就静默返回；
			// 否则是真正的意外错误，打印出来再返回。
			select {
			case <-ctx.Done():
				return
			default:
				output.Printf("accept failed: %v\n", err)
				return
			}
		}

		// 把新连接登记为“入站”，拿到分配的 ID。
		id, err := manager.addInbound(conn)
		if err != nil {
			// 登记失败就把这条连接关掉别泄漏，continue 继续接收下一个（不退出循环）。
			_ = CloseConnection(conn)
			output.Printf("accept failed: %v\n", err)
			continue
		}
		output.Printf("accepted %s from %s\n", id, conn.RemoteAddr().String())
	}
}

/**************************************************************
*
下面是主程序命令行的各个命令的处理函数，包括 reach、close 和 list 命令。
*
**************************************************************/

func handleReach(ctx context.Context, cfg CommandLineConfig, manager *connectionManager, output *lockedWriter, fields []string) {
	// reach 命令要求正好两个词：reach 和目标地址，否则打印用法提示。
	if len(fields) != 2 {
		output.Print("usage: reach <ip:port>\n")
		return
	}

	var dialCfg DialConfig
	dialCfg.PeerAddr = strings.TrimSpace(fields[1])
	dialCfg.DialTimeout = cfg.DialTimeout

	// 主动向对端发起 TCP 连接。
	conn, err := DialConnection(ctx, dialCfg)
	if err != nil {
		output.Printf("reach failed: %v\n", err)
		return
	}

	// 连接成功后登记为“出站”连接。
	id, err := manager.addOutbound(conn)
	if err != nil {
		// 登记失败要把刚建立的连接关掉，防止资源泄漏。
		_ = CloseConnection(conn)
		output.Printf("reach failed: %v\n", err)
		return
	}

	// 成功则把连接 ID 回显给用户，方便后续 close 使用。
	output.Printf("%s\n", id)
}

func handleClose(manager *connectionManager, output *lockedWriter, fields []string) {
	if len(fields) != 2 {
		output.Print("usage: close <id>\n")
		return
	}

	// 取第二个词作为要关闭的连接 ID。
	id := strings.TrimSpace(fields[1])
	// if 语句里可以直接声明并使用变量：err 的作用域仅限这个 if。
	if err := manager.close(id); err != nil {
		output.Printf("close failed: %v\n", err)
		return
	}
	output.Printf("closed %s\n", id)
}

func handleList(manager *connectionManager, output *lockedWriter, fields []string) {
	if len(fields) != 2 {
		output.Print("usage: list connections\n")
		return
	}
	// 目前只支持 "list connections" 这一种子命令，其它子类型报错。
	if fields[1] != "connections" {
		output.Printf("unknown list type: %s\n", fields[1])
		return
	}

	// 拿到已排序的连接快照，逐行按 [ID][发起方][接收方] 格式打印。
	items := manager.list()
	for _, item := range items {
		output.Printf("[%s][%s][%s]\n", item.ID, item.Initiator, item.Receiver)
	}
}
