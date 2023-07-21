package tcp

/**
 * A echo server to test whether the server is functioning normally
 */

import (
	"bufio"
	"context"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

// MakeEchoHandler creates EchoHandler
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler, using for test
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close close connection
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// Handle echos received line to client
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() { //如果服务器关闭则关闭连接
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{}) //放到现成同步的set中(map实现）

	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF, client timeout, server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		//logger.Info("sleeping")
		//time.Sleep(10 * time.Second)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

// Close stops echo handler
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)                                              //设置标识
	h.activeConn.Range(func(key interface{}, val interface{}) bool { //遍历所有连接并关闭
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
