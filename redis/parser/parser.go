package parser

import (
	"bufio"                                    // 导入Go语言的缓冲IO包
	"bytes"                                    // 导入Go语言的bytes包，处理字节切片
	"errors"                                   // 导入Go语言的errors包，处理错误
	"github.com/hdt3213/godis/interface/redis" // 导入第三方包redis，处理redis命令
	"github.com/hdt3213/godis/lib/logger"      // 导入第三方包logger，记录日志
	"github.com/hdt3213/godis/redis/protocol"  // 导入第三方包protocol，处理redis协议
	"io"                                       // 导入Go语言的io包，处理IO操作
	"runtime/debug"                            // 导入Go语言的runtime/debug包，处理运行时的调试信息
	"strconv"                                  // 导入Go语言的strconv包，处理字符串和其他数据类型的转换
	"strings"                                  // 导入Go语言的strings包，处理字符串
)

// Payload是一个结构体，它存储了redis的响应或者错误
type Payload struct {
	Data redis.Reply // 数据，是redis的响应
	Err  error       // 错误信息
}

// ParseStream从io.Reader读取数据，并通过通道发送payloads
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload) // 创建一个Payload类型的通道
	go parse0(reader, ch)     // 开启一个goroutine，从io.Reader读取数据并解析
	return ch                 // 返回这个通道，用于接收数据
}

// ParseBytes从[]byte读取数据，并返回所有的响应
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)       // 创建一个Payload类型的通道
	reader := bytes.NewReader(data) // 创建一个从字节切片读取数据的Reader
	go parse0(reader, ch)           // 开启一个goroutine，从Reader读取数据并解析
	var results []redis.Reply       // 用于存储所有的响应
	for payload := range ch {       // 从通道中读取Payload
		if payload == nil { // 如果Payload是nil，表示没有协议
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil { // 如果Payload中存在错误
			if payload.Err == io.EOF { // 如果错误是EOF，表示所有数据已经读取完毕
				break
			}
			return nil, payload.Err // 返回错误
		}
		results = append(results, payload.Data) // 将Payload的数据添加到结果中
	}
	return results, nil // 返回所有的结果和nil错误
}

// ParseOne从[]byte读取数据，并返回第一个payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)       // 创建一个Payload类型的通道
	reader := bytes.NewReader(data) // 创建一个从字节切片读取数据的Reader
	go parse0(reader, ch)           // 开启一个goroutine，从Reader读取数据并解析
	payload := <-ch                 // 从通道中读取第一个Payload，parse0会关闭通道
	if payload == nil {             // 如果Payload是nil，表示没有协议
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err // 返回Payload的数据和错误
}

// parse0 函数负责解析输入流，并将解析结果发送到 Payload 类型的通道 ch 中
func parse0(rawReader io.Reader, ch chan<- *Payload) {
	// 使用 defer 和 recover 捕获和处理运行时发生的 panic，以避免程序崩溃
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack())) // 记录错误信息和堆栈跟踪
		}
	}()

	// 创建一个新的带缓冲的 reader
	reader := bufio.NewReader(rawReader)
	// 开始无限循环，逐行读取数据
	for {
		// 读取一行数据，直到遇到换行符 '\n'
		line, err := reader.ReadBytes('\n')
		// 如果在读取过程中出错，则将错误信息发送到通道 ch，并关闭通道，然后返回
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		// 获取读取到的行的长度
		length := len(line)
		// 如果行的长度小于或等于2，或者行尾第二个字符不是 '\r'，则跳过此次循环
		// 在复制流量中可能会有一些空行，此处忽略这种错误
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		// 去除行尾的 '\r' 和 '\n'
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})

		// 根据行首的字符判断该行数据的类型
		switch line[0] {
		case '+': // 如果行首字符为 '+'
			// 解析出该行数据的内容（不包含行首的 '+'）
			content := string(line[1:])
			// 将解析出的内容作为状态回复发送到通道 ch
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
			// 如果解析出的内容以 "FULLRESYNC" 开头，则继续解析 RDB 批量字符串
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch)
				// 如果在解析过程中出错，则将错误信息发送到通道 ch，并关闭通道，然后返回
				if err != nil {
					ch <- &Payload{Err: err}
					close(ch)
					return
				}
			}
		case '-': // 如果行首字符为 '-'
			// 解析出该行数据的内容（不包含行首的 '-'），并作为错误回复发送到通道 ch
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':': // 如果行首字符为 ':'
			// 解析出该行数据的内容（不包含行首的 ':'），并尝试将其转换为 int64 类型的值
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			// 如果在转换过程中出错，则将错误信息发送到通道 ch，然后跳过此次循环
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			// 将转换后的值作为整数回复发送到通道 ch
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$': // 如果行首字符为 '$'
			// 解析批量字符串
			err = parseBulkString(line, reader, ch)
			// 如果在解析过程中出错，则将错误信息发送到通道 ch，并关闭通道，然后返回
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*': // 如果行首字符为 '*'
			// 解析数组
			err = parseArray(line, reader, ch)
			// 如果在解析过程中出错，则将错误信息发送到通道 ch，并关闭通道，然后返回
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default: // 如果行首字符为其他字符
			// 解析出该行数据的所有参数，并将其作为多批量回复发送到通道 ch
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

// parseBulkString 函数负责解析批量字符串，并将解析结果发送到 Payload 类型的通道 ch 中
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 尝试将 header[1:] 转换为 int64 类型的值
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	// 如果在转换过程中出错，或者转换后的值小于 -1，则将错误信息发送到通道 ch，然后返回
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		// 如果转换后的值等于 -1，则将一个空的批量回复发送到通道 ch，然后返回
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}

	// 创建一个新的字节切片 body，并从 reader 中读取 strLen+2 个字节的数据
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	// 如果在读取过程中出错，则返回错误
	if err != nil {
		return err
	}
	// 将读取到的数据（除最后两个字节外）作为批量回复发送到通道 ch
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// parseRDBBulkString 函数负责解析 RDB 批量字符串，并将解析结果发送到 Payload 类型的通道 ch 中
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	// 读取一行数据，作为 header
	header, err := reader.ReadBytes('\n')
	// 去除 header 尾部的 '\r' 和 '\n'
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	// 如果 header 为空，则返回错误
	if len(header) == 0 {
		return errors.New("empty header")
	}
	// 尝试将 header[1:] 转换为 int64 类型的值
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	// 如果在转换过程中出错，或者转换后的值小于或等于 0，则返回错误
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}

	// 创建一个新的字节切片 body，并从 reader 中读取 strLen 个字节的数据
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	// 如果在读取过程中出错，则返回错误
	if err != nil {
		return err
	}
	// 将读取到的数据作为批量回复发送到通道 ch
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

// parseArray 函数负责解析数组，并将解析结果发送到 Payload 类型的通道 ch 中
func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 尝试将 header[1:] 转换为 int64 类型的值
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	// 如果在转换过程中出错，或者转换后的值小于 0，则将错误信息发送到通道 ch，然后返回
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		// 如果转换后的值等于 0，则将一个空的多批量回复发送到通道 ch，然后返回
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}

	// 创建一个新的字节切片切片 lines，用于存储数组中的所有元素
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		// 读取一行数据
		var line []byte
		line, err = reader.ReadBytes('\n')
		// 如果在读取过程中出错，则返回错误
		if err != nil {
			return err
		}
		length := len(line)
		// 如果行的长度小于 4，或者行尾第二个字符不是 '\r'，或者行首字符不是 '$'，则将错误信息发送到通道 ch，然后退出循环
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}

		// 尝试将 line[1:length-2] 转换为 int64 类型的值
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		// 如果在转换过程中出错，或者转换后的值小于 -1，则将错误信息发送到通道 ch，然后退出循环
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			// 如果转换后的值等于 -1，则向 lines 中添加一个空的字节切片
			lines = append(lines, []byte{})
		} else {
			// 创建一个新的字节切片 body，并从 reader 中读取 strLen+2 个字节的数据
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			// 如果在读取过程中出错，则返回错误
			if err != nil {
				return err
			}
			// 向 lines 中添加读取到的数据（除最后两个字节外）
			lines = append(lines, body[:len(body)-2])
		}
	}
	// 将 lines 作为多批量回复发送到通道 ch
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

// protocolError 函数负责将错误信息发送到 Payload 类型的通道 ch
func protocolError(ch chan<- *Payload, msg string) {
	// 创建一个新的错误
	err := errors.New("protocol error: " + msg)
	// 将创建的错误发送到通道 ch
	ch <- &Payload{Err: err}
}
