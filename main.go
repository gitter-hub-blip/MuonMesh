package main

import (
	"context"
	"flag"
	"log"
	"os"
)

func main() {
	// 主程序只负责读取参数和组织流程，参数检查交给各模块自己的 API。
	listenAddr := flag.String("listen", "127.0.0.1:9000", "local TCP address to receive on")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var cfg CommandLineConfig
	cfg.ListenAddr = *listenAddr
	cfg.Input = os.Stdin
	cfg.Output = os.Stdout

	err := RunCommandLine(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
}
