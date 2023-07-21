package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current Godis server
var ClientCounter int

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
/**
 * @Description: 通过信号监听端口
 * @param cf: 配置文件
 * @param handler: TCP处理器
 * @return error: 错误
 * @process: 1. 创建信号通道 2. 监听信号 3. 创建监听器 4. 监听端口 5. 处理连接
 */
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
/**
 * @Description: 监听端口
 * @param listener: 监听器
 * @param handler: TCP处理器
 * @param closeChan: 关闭信息通道
 * @process: 1. 如果有关闭信息或者错误信息就关闭 2. 接受连接并添加相关信息 3. 处理连接
 */
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			break
		}
		// handle
		logger.Info("accept link")
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				ClientCounter--
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
