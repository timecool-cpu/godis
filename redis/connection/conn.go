package connection // 定义名为connection的包

import ( // 导入所需要的包
	"github.com/hdt3213/godis/lib/logger"    // 导入日志相关的包
	"github.com/hdt3213/godis/lib/sync/wait" // 导入同步等待的包
	"net"                                    // 导入网络相关的包
	"sync"                                   // 导入同步机制相关的包
	"time"                                   // 导入时间处理相关的包
)

const ( // 定义一些常量
	// flagSlave 表示这是一个slave连接
	flagSlave = uint64(1 << iota)
	// flagMaster 表示这是一个master连接
	flagMaster
	// flagMulti 表示这个连接处于一个事务中
	flagMulti
)

// Connection 表示一个与redis-cli的连接
type Connection struct {
	conn net.Conn // 代表一个网络连接

	// 用于优雅的关闭，在发送数据完成后等待
	sendingData wait.Wait

	// 在服务器发送响应时加锁
	mu    sync.Mutex
	flags uint64

	// 订阅的频道
	subs map[string]bool

	// 密码可能在运行时由CONFIG命令更改，所以存储密码
	password string

	// 为`multi`命令队列化的命令
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// 选定的数据库
	selectedDB int
}

var connPool = sync.Pool{ // 创建一个连接池
	New: func() interface{} {
		return &Connection{} // 当需要新的连接时，返回一个新的Connection结构体
	},
}

// RemoteAddr 返回远程网络地址
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String() // 返回连接的远程网络地址
}

// Close 与客户端断开连接
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second) // 等待发送数据完成或超时
	_ = c.conn.Close()                              // 关闭网络连接
	c.subs = nil                                    // 清空订阅的频道
	c.password = ""                                 // 清空密码
	c.queue = nil                                   // 清空命令队列
	c.watching = nil                                // 清空watching列表
	c.txErrors = nil                                // 清空事务错误
	c.selectedDB = 0                                // 重置选定的数据库为0
	connPool.Put(c)                                 // 将连接放回连接池
	return nil
}

// NewConn 创建Connection实例
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection) // 从连接池中获取一个连接
	if !ok {                              // 如果连接池中没有可用的连接
		logger.Error("connection pool make wrong type") // 记录错误
		return &Connection{                             // 返回一个新的Connection实例
			conn: conn,
		}
	}
	c.conn = conn // 设置网络连接
	return c      // 返回连接实例
}

// Write 将响应通过tcp连接发送给客户端
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 { // 如果没有需要发送的数据
		return 0, nil // 直接返回
	}
	c.sendingData.Add(1) // 开始发送数据，增加等待计数
	defer func() {       // 在函数结束时
		c.sendingData.Done() // 减少等待计数
	}()

	return c.conn.Write(b) // 将数据写入到连接中，并返回写入的字节数和可能的错误
}

func (c *Connection) Name() string {
	if c.conn != nil { // 如果存在一个网络连接
		return c.conn.RemoteAddr().String() // 返回连接的远程地址
	}
	return "" // 如果没有网络连接，返回空字符串
}

// Subscribe 将当前连接加入到给定频道的订阅者中
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()         // 加锁
	defer c.mu.Unlock() // 在函数结束时解锁

	if c.subs == nil { // 如果还没有订阅任何频道
		c.subs = make(map[string]bool) // 初始化订阅频道的映射
	}
	c.subs[channel] = true // 添加新的订阅频道
}

// UnSubscribe 将当前连接从给定频道的订阅者中移除
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()         // 加锁
	defer c.mu.Unlock() // 在函数结束时解锁

	if len(c.subs) == 0 { // 如果没有订阅任何频道
		return // 直接返回
	}
	delete(c.subs, channel) // 从订阅的频道中移除给定的频道
}

// SubsCount 返回订阅频道的数量
func (c *Connection) SubsCount() int {
	return len(c.subs) // 返回订阅频道的数量
}

// GetChannels 返回所有订阅的频道
func (c *Connection) GetChannels() []string {
	if c.subs == nil { // 如果没有订阅任何频道
		return make([]string, 0) // 返回一个空的频道列表
	}
	channels := make([]string, len(c.subs)) // 创建一个足够大的切片来存储所有的频道
	i := 0                                  // 定义一个计数器
	for channel := range c.subs {           // 遍历订阅的所有频道
		channels[i] = channel // 添加到切片中
		i++                   // 计数器加1
	}
	return channels // 返回频道列表
}

// SetPassword 为认证存储密码
func (c *Connection) SetPassword(password string) {
	c.password = password // 设置密码
}

// GetPassword 获取认证的密码
func (c *Connection) GetPassword() string {
	return c.password // 返回密码
}

// InMultiState 检查连接是否在一个未提交的事务中
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0 // 检查连接的标志位中是否包含flagMulti
}

// SetMultiState 设置事务标志
func (c *Connection) SetMultiState(state bool) {
	if !state { // 如果状态为false
		c.watching = nil      // 清空watching列表
		c.queue = nil         // 清空命令队列
		c.flags &= ^flagMulti // 清除multi标志位
		return
	}
	c.flags |= flagMulti // 设置multi标志位
}

// GetQueuedCmdLine 返回当前事务的排队命令
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue // 返回命令队列
}

// EnqueueCmd  将当前事务的命令加入队列
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine) // 将命令添加到命令队列的末尾
}

// AddTxError 存储事务中的语法错误
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err) // 将错误添加到事务错误的列表中
}

// GetTxErrors 返回事务中的语法错误
func (c *Connection) GetTxErrors() []error {
	return c.txErrors // 返回事务错误的列表
}

// ClearQueuedCmds 清空当前事务的排队命令
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil // 清空命令队列
}

// GetWatching 返回开始观察时的观察键和它们的版本代码
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil { // 如果watching列表为空
		c.watching = make(map[string]uint32) // 初始化watching映射
	}
	return c.watching // 返回watching映射
}

// GetDBIndex 返回选定的数据库
func (c *Connection) GetDBIndex() int {
	return c.selectedDB // 返回选定的数据库编号
}

// SelectDB 选择一个数据库
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum // 设置选定的数据库编号
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave // 设置slave标志位
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0 // 检查连接的标志位中是否包含flagSlave
}

func (c *Connection) SetMaster() {
	c.flags |= flagMaster // 设置master标志位
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0 // 检查连接的标志位中是否包含flagMaster
}
