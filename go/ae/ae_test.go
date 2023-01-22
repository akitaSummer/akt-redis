package ae

import (
	"akt-redis/net"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func WriteCallback(loop *AeLoop, fd int, extra interface{}) {
	buf := extra.([]byte)
	n, err := net.Write(fd, buf)
	if err != nil {
		fmt.Printf("write err: %v\n", err)
		return
	}
	fmt.Printf("ae write %v bytes\n", n)
	loop.RemoveFileEvent(fd, AE_WRITABLE)
}

func ReadCallback(loop *AeLoop, fd int, extra interface{}) {
	buf := make([]byte, 10)
	n, err := net.Read(fd, buf)
	if err != nil {
		fmt.Printf("read err: %v\n", err)
		return
	}
	fmt.Printf("ae read %v bytes\n", n)
	loop.AddFileEvent(fd, AE_WRITABLE, WriteCallback, buf)
}

func AcceptCallback(loop *AeLoop, fd int, extra interface{}) {
	cfd, err := net.Accept(fd)
	if err != nil {
		fmt.Printf("accept err: %v\n", err)
		return
	}
	loop.AddFileEvent(cfd, AE_READABLE, ReadCallback, nil)
}

func OnceCallback(loop *AeLoop, id int, extra interface{}) {
	t := extra.(*testing.T)
	assert.Equal(t, 1, id)
	fmt.Printf("time event %v done\n", id)
}

func NormalCallback(loop *AeLoop, id int, extra interface{}) {
	end := extra.(chan struct{})
	fmt.Printf("time event %v done\n", id)
	end <- struct{}{}
}

func TestAe(t *testing.T) {
	loop, err := AeLoopCreate()
	assert.Nil(t, err)
	sfd, err := net.TcpServer(6666)
	loop.AddFileEvent(sfd, AE_READABLE, AcceptCallback, nil)
	go loop.AeMain()

	host := [4]byte{0, 0, 0, 0}
	cfd, err := net.Connect(host, 6666)
	assert.Nil(t, err)
	msg := "helloworld"
	n, err := net.Write(cfd, []byte(msg))
	assert.Nil(t, err)
	assert.Equal(t, 10, n)
	buf := make([]byte, 10)
	n, err = net.Read(cfd, buf)
	assert.Nil(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, msg, string(buf))

	loop.AddTimeEvent(AE_ONCE, 10, OnceCallback, t)
	end := make(chan struct{}, 2)
	loop.AddTimeEvent(AE_NORMAL, 10, NormalCallback, end)
	<-end
	<-end
	loop.stop = true
}
