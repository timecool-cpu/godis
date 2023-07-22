package server // 定义了包名为 server

/*
 * A tcp.Handler implements redis protocol
 */

import ( // 导入必要的包
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/hdt3213/godis/cluster"            // 导入的外部包，用于实现 Redis 集群功能
	"github.com/hdt3213/godis/config"             // 导入的外部包，用于处理配置文件
	database2 "github.com/hdt3213/godis/database" // 导入的外部包，用于数据库操作
	"github.com/hdt3213/godis/interface/database" // 导入的外部包，定义了数据库接口
	"github.com/hdt3213/godis/lib/logger"         // 导入的外部包，用于日志记录
	"github.com/hdt3213/godis/lib/sync/atomic"    // 导入的外部包，提供了一些并发控制的原子操作
	"github.com/hdt3213/godis/redis/connection"   // 导入的外部包，用于处理 Redis 客户端连接
	"github.com/hdt3213/godis/redis/parser"       // 导入的外部包，用于解析 Redis 协议
	"github.com/hdt3213/godis/redis/protocol"     // 导入的外部包，用于处理 Redis 协议
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n") // 未知错误的默认回复
)

// Handler 实现了 tcp.Handler 接口并充当 Redis 服务器
type Handler struct {
	activeConn sync.Map       // *client -> placeholder, 用于存储当前活跃的客户端连接
	db         database.DB    // 数据库实例
	closing    atomic.Boolean // 表示是否正在关闭服务器的标志
}

// MakeHandler 创建一个 Handler 实例
func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster() // 如果开启了集群模式，创建集群实例
	} else {
		db = database2.NewStandaloneServer() // 否则，创建单机实例
	}
	return &Handler{ // 返回 Handler 实例
		db: db,
	}
}

// 当需要关闭客户端连接时调用此方法
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()            // 关闭连接
	h.db.AfterClientClose(client) // 在数据库中进行一些清理操作
	h.activeConn.Delete(client)   // 从活跃连接列表中删除该客户端
}

// Handle 方法用于接收和执行 Redis 命令
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() { // 如果服务器正在关闭，拒绝新的连接
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)     // 封装 conn 为 client
	h.activeConn.Store(client, struct{}{}) // 将新的客户端添加到活跃连接列表中

	ch := parser.ParseStream(conn) // 解析客户端发送的命令
	for payload := range ch {      // 遍历每一个解析出的 payload
		// 如果 payload 中存在错误
		if payload.Err != nil {
			// 如果错误类型是 EOF，表示客户端已经关闭了连接
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error()) // 制造一个错误回复
			_, err := client.Write(errReply.ToBytes())             // 将错误回复写回到客户端
			if err != nil {
				h.closeClient(client) // 如果写入过程中出现错误，关闭该客户端
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		// 如果 payload 中的数据为空
		if payload.Data == nil {
			logger.Error("empty payload") // 记录一条错误日志
			continue
		}
		// 如果 payload 中的数据不是 MultiBulkReply 类型
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol") // 记录一条错误日志
			continue
		}
		// 在数据库上执行解析出的命令
		result := h.db.Exec(client, r.Args)
		if result != nil { // 如果结果不为空
			_, _ = client.Write(result.ToBytes()) // 将结果写回到客户端
		} else { // 如果结果为空
			_, _ = client.Write(unknownErrReplyBytes) // 将默认的未知错误回复写回到客户端
		}
	}
}

// Close 停止 Handler 的执行
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true) // 设置关闭标志为真
	// TODO: concurrent wait
	// 关闭所有活跃的客户端连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close() // 关闭数据库
	return nil
}
