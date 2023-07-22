package client

import (
	"errors"                                   // 用于创建标准的错误
	"github.com/hdt3213/godis/interface/redis" // Redis接口定义
	"github.com/hdt3213/godis/lib/logger"      // 日志库
	"github.com/hdt3213/godis/lib/sync/wait"   // 等待功能库
	"github.com/hdt3213/godis/redis/parser"    // Redis协议解析器
	"github.com/hdt3213/godis/redis/protocol"  // Redis协议库
	"net"                                      // 内置的网络库，用于创建网络连接
	"runtime/debug"                            // 用于处理运行时的调试信息
	"strings"                                  // 内置的字符串处理库
	"sync"                                     // 内置的同步库，包含各种并发原语
	"sync/atomic"                              // 内置的原子操作库
	"time"                                     // 内置的时间库
)

const (
	created = iota // 客户端状态：已创建
	running        // 客户端状态：运行中
	closed         // 客户端状态：已关闭
)

// Client 是一个Redis客户端
type Client struct {
	conn        net.Conn      // 与Redis服务器的连接
	pendingReqs chan *request // 待发送的请求
	waitingReqs chan *request // 正在等待响应的请求
	ticker      *time.Ticker  // 定时器，用于发送心跳
	addr        string        // Redis服务器的地址

	status  int32           // 客户端的状态
	working *sync.WaitGroup // 计数器，表示未完成的请求
}

// request 是发送到Redis服务器的消息
type request struct {
	id        uint64      // 请求ID
	args      [][]byte    // 请求参数
	reply     redis.Reply // 响应
	heartbeat bool        // 是否是心跳请求
	waiting   *wait.Wait  // 请求等待完成的通知
	err       error       // 请求错误
}

const (
	chanSize = 256             // 通道的大小
	maxWait  = 3 * time.Second // 最大等待时间
)

// MakeClient 创建一个新的客户端
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr) // 连接到Redis服务器
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start 启动异步的goroutine
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite() // 启动写处理器
	go client.handleRead()  // 启动读处理器
	go client.heartbeat()   // 启动心跳
	atomic.StoreInt32(&client.status, running)
}

// Close函数定义：它会停止异步的goroutine并关闭连接
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed) // 将客户端的状态设置为关闭
	client.ticker.Stop()                      // 停止计时器
	close(client.pendingReqs)                 // 关闭正在等待发送的请求通道，阻止新请求

	client.working.Wait() // 阻塞，直到所有未完成的工作都已完成

	_ = client.conn.Close()   // 关闭网络连接，忽略任何可能出现的错误
	close(client.waitingReqs) // 关闭等待响应的请求通道
}

// 重连函数定义：如果连接丢失，它会尝试重新建立连接
func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr) // 记录要重连的服务器地址
	_ = client.conn.Close()                       // 关闭旧的连接，忽略可能的错误

	var conn net.Conn
	for i := 0; i < 3; i++ { // 重试3次
		var err error
		conn, err = net.Dial("tcp", client.addr) // 尝试建立新的连接
		if err != nil {                          // 如果连接失败
			logger.Error("reconnect error: " + err.Error()) // 记录错误信息
			time.Sleep(time.Second)                         // 等待1秒后重试
			continue
		} else {
			break // 连接成功，跳出循环
		}
	}
	if conn == nil { // 如果还是无法连接，关闭客户端
		client.Close()
		return
	}
	client.conn = conn // 使用新的连接

	close(client.waitingReqs)             // 关闭旧的等待响应请求通道
	for req := range client.waitingReqs { // 遍历旧的等待响应请求通道中的请求
		req.err = errors.New("connection closed") // 设置错误信息
		req.waiting.Done()                        // 将wait组的计数器减1，表示请求已处理完毕
	}
	client.waitingReqs = make(chan *request, chanSize) // 创建新的等待响应请求通道
	go client.handleRead()                             // 启动新的读取处理goroutine
}

// 心跳函数定义：它会定期发送心跳请求以保持连接的活跃
func (client *Client) heartbeat() {
	for range client.ticker.C { // 定时器每次触发时
		client.doHeartbeat() // 发送心跳请求
	}
}

// 处理写请求的函数定义：它会处理所有等待发送的请求
func (client *Client) handleWrite() {
	for req := range client.pendingReqs { // 对于每一个等待发送的请求
		client.doRequest(req) // 执行该请求
	}
}

// 发送函数定义：它会发送一个请求到redis服务器
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running { // 如果客户端状态不是运行中，则返回错误
		return protocol.MakeErrReply("client closed")
	}
	req := &request{ // 创建新的请求
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)                              // 增加等待组的计数器
	client.working.Add(1)                           // 增加工作组的计数器
	defer client.working.Done()                     // 在函数返回时，减少工作组的计数器
	client.pendingReqs <- req                       // 将请求添加到等待发送的请求通道
	timeout := req.waiting.WaitWithTimeout(maxWait) // 等待请求完成或者超时
	if timeout {                                    // 如果超时，则返回错误
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil { // 如果请求出错，则返回错误
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply // 返回请求的响应
}

// 处理心跳请求的函数定义：它会发送一个心跳请求到服务器并等待响应
func (client *Client) doHeartbeat() {
	request := &request{ // 创建心跳请求
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)                   // 增加等待组的计数器
	client.working.Add(1)                    // 增加工作组的计数器
	defer client.working.Done()              // 在函数返回时，减少工作组的计数器
	client.pendingReqs <- request            // 将心跳请求添加到等待发送的请求通道
	request.waiting.WaitWithTimeout(maxWait) // 等待请求完成或者超时
}

// 执行请求的函数定义：它会发送一个请求到redis服务器并添加到等待响应的请求通道
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 { // 如果请求不存在或者请求参数为空，则返回
		return
	}
	re := protocol.MakeMultiBulkReply(req.args) // 根据请求参数创建redis请求
	bytes := re.ToBytes()                       // 将请求转换为字节
	var err error
	for i := 0; i < 3; i++ { // 最多重试3次
		_, err = client.conn.Write(bytes) // 发送请求
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // 如果不是超时错误
				!strings.Contains(err.Error(), "deadline exceeded")) { // 或者不是超过期限的错误，则跳出循环
			break
		}
	}
	if err == nil { // 如果请求发送成功
		client.waitingReqs <- req // 将请求添加到等待响应的请求通道
	} else { // 如果请求发送失败
		req.err = err      // 设置错误信息
		req.waiting.Done() // 将等待组的计数器减1，表示请求已处理完毕
	}
}

// 完成请求的函数定义：它会从等待响应的请求通道中取出一个请求并设置响应
func (client *Client) finishRequest(reply redis.Reply) {
	defer func() { // 在函数返回时，恢复任何可能出现的panic
		if err := recover(); err != nil {
			debug.PrintStack() // 打印堆栈跟踪
			logger.Error(err)  // 记录错误信息
		}
	}()
	request := <-client.waitingReqs // 从等待响应的请求通道中取出一个请，因为是按照顺序的，所有不需要考虑请求的顺序
	if request == nil {             // 如果请求不存在，则返回
		return
	}
	request.reply = reply       // 设置请求的响应
	if request.waiting != nil { // 如果等待组存在
		request.waiting.Done() // 将等待组的计数器减1，表示请求已处理完毕
	}
}

// 处理读取的函数定义：它会接收来自服务器的响应并将其匹配到相应的请求
func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn) // 解析服务器发送的响应流
	for payload := range ch {             // 对于每一个响应
		if payload.Err != nil { // 如果响应出错
			status := atomic.LoadInt32(&client.status) // 读取客户端的状态
			if status == closed {                      // 如果客户端已关闭，则返回
				return
			}
			client.reconnect() // 否则，尝试重新连接
			return
		}
		client.finishRequest(payload.Data) // 将响应匹配到相应的请求
	}
}
