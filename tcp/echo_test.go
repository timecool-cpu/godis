package tcp

import (
	"bufio"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n")) // 客户端发送数据
		if err != nil {
			t.Error(err)
			return
		}
		bufReader := bufio.NewReader(conn) // 客户端接收数据
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		if string(line) != val { // 判断是否收到相同的数据
			t.Error("get wrong response")
			return
		}
	}
	_ = conn.Close()
	for i := 0; i < 5; i++ {
		// create idle connection
		_, _ = net.Dial("tcp", addr)
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
