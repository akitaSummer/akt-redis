package net

import (
	"log"

	"golang.org/x/sys/unix"
)

const BACKLOG int = 64

func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
}

func Close(fd int) {
	unix.Close(fd)
}

func Connect(host [4]byte, port int) (int, error) {
	// 创建socket
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket err: %v\n", err)
		return -1, err
	}
	// 地址绑定
	var addr unix.SockaddrInet4
	addr.Addr = host
	addr.Port = port
	// 链接
	err = unix.Connect(s, &addr)
	if err != nil {
		log.Printf("connect err: %v\n", err)
		return -1, err
	}
	return s, nil
}

func TcpServer(port int) (int, error) {
	// 创建socket
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket err: %v\n", err)
		return -1, nil
	}
	// 设置正在使用及端口复用
	err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEPORT, port)
	if err != nil {
		log.Printf("set SO_REUSEPORT err: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	// 地址绑定
	var addr unix.SockaddrInet4
	addr.Port = port
	err = unix.Bind(s, &addr)
	if err != nil {
		log.Printf("bind addr err: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	err = unix.Listen(s, BACKLOG)
	if err != nil {
		log.Printf("listen socket err: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	return s, nil
}

func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}

func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}
