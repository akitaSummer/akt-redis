package main

import (
	"akt-redis/ae"
	"akt-redis/conf"
	"akt-redis/dict"
	"akt-redis/list"
	"akt-redis/net"
	"akt-redis/obj"
	"errors"
	"strings"
	"time"

	"hash/fnv"
	"log"
	"os"
)

type CmdType = byte

const (
	COMMAND_UNKNOWN CmdType = 0x00
	COMMAND_INLINE  CmdType = 0x01
	COMMAND_BULK    CmdType = 0x02
)

const (
	GODIS_IO_BUF     int = 1024 * 16
	GODIS_MAX_BULK   int = 1024 * 4
	GODIS_MAX_INLINE int = 1024 * 4
)

type GodisDB struct {
	data   *dict.Dict
	expire *dict.Dict
}

type GodisServer struct {
	fd      int
	port    int
	db      *GodisDB
	clients map[int]*GodisClient
	aeLoop  *ae.AeLoop
}

type GodisClient struct {
	fd       int
	db       *GodisDB
	args     []*obj.Gobj
	reply    *list.List
	queryBuf []byte
	queryLen int
	cmdType  CmdType
	bulkNum  int
	bulkLen  int
	sentLen  int
}

type CommandProc func(client *GodisClient)

type GodisCommand struct {
	name  string
	proc  CommandProc
	arity int // 参数个数
}

func (client *GodisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > GODIS_MAX_INLINE {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

var server GodisServer

var cmdTable []GodisCommand = []GodisCommand{
	{"get", func(client *GodisClient) {}, 2},
	{"set", func(client *GodisClient) {}, 3},
	{"expire", func(client *GodisClient) {}, 3},
	//TODO
}

func GStrEqual(a, b *obj.Gobj) bool {
	if a.Type_ != obj.GSTR || b.Type_ != obj.GSTR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

func GStrHash(key *obj.Gobj) int64 {
	if key.Type_ != obj.GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}

func CreateClient(fd int) *GodisClient {
	var client GodisClient
	client.fd = fd
	client.db = server.db
	client.queryBuf = make([]byte, GODIS_IO_BUF)
	client.reply = list.ListCreate(list.ListType{EqualFunc: GStrEqual})
	return &client
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdType = COMMAND_UNKNOWN
	client.bulkLen = 0
	client.bulkNum = 0
}

// 释放client.args中的gobj
func freeArgs(client *GodisClient) {
	for _, v := range client.args {
		v.DecrRefCount()
	}
}

func freeReplyList(client *GodisClient) {
	for client.reply.Length() != 0 {
		n := client.reply.Head
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}

// 释放client
func freeClient(client *GodisClient) {
	freeArgs(client)
	// 从map表中删除
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, ae.AE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, ae.AE_WRITABLE)
	freeReplyList(client)
	net.Close(client.fd)
}

// 通知client
func SendReplyToClient(loop *ae.AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := net.Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, ae.AE_WRITABLE)
	}
}

func (c *GodisClient) AddReply(o *obj.Gobj) {
	c.reply.Append(o)
	o.IncrRefCount()
	server.aeLoop.AddFileEvent(c.fd, ae.AE_WRITABLE, SendReplyToClient, c)
}

func (c *GodisClient) AddReplyStr(str string) {
	o := obj.CreateObject(obj.GSTR, str)
	c.AddReply(o)
	o.DecrRefCount()
}

// 寻找对应的cmd
func lookupCommand(cmdStr string) *GodisCommand {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}

// 执行cmd
func ProcessCommand(client *GodisClient) {
	cmdStr := client.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		freeClient(client)
		return
	}
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		client.AddReplyStr("-ERR: unknow command")
		resetClient(client)
		return
	} else if cmd.arity != len(client.args) {
		client.AddReplyStr("-ERR: wrong number of args")
		resetClient(client)
		return
	}
	cmd.proc(client)
	resetClient(client)
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index, err := client.findLineInQuery()
	if index < 0 {
		return false, err
	}

	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
	client.args = make([]*obj.Gobj, len(subs))

	for i, v := range subs {
		client.args[i] = obj.CreateObject(obj.GSTR, v)
	}
	return true, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	// TODO
	return true, nil
}

// 处理client query
func ProcessQueryBuf(client *GodisClient) error {
	// 不断取值
	for len(client.queryBuf) > 0 {
		if client.cmdType == COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdType = COMMAND_BULK
			} else {
				client.cmdType = COMMAND_INLINE
			}
		}

		// 读取内容转化为cmd args
		var ok bool
		var err error
		if client.cmdType == COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdType == COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknow Godis Command Type")
		}
		if err != nil {
			return err
		}

		// 执行cmd
		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			// cmd incomplete
			break
		}
	}
	return nil
}

func ReadQueryFromClient(loop *ae.AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	if len(client.queryBuf)-client.queryLen < GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, GODIS_MAX_BULK)...)
	}
	n, err := net.Read(fd, client.queryBuf[client.queryLen:])

	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		freeClient(client)
		return
	}

	client.queryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	// 处理query
	err = ProcessQueryBuf(client)
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

// 接受client
func AcceptHandler(loop *ae.AeLoop, fd int, extra interface{}) {
	cfd, err := net.Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}

	client := CreateClient(cfd)
	//TODO: check max clients limit
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, ae.AE_READABLE, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

// 随机取数据判断是否过期
func ServerCron(loop *ae.AeLoop, id int, extra interface{}) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Val.IntVal() < time.Now().Unix() {
			server.db.data.Delete(entry.Key)
			server.db.expire.Delete(entry.Key)
		}
	}
}

// 初始化godis server
func initServer(config *conf.Config) error {
	server.port = config.Port
	server.clients = make(map[int]*GodisClient)
	server.db = &GodisDB{
		data:   dict.DictCreate(dict.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: dict.DictCreate(dict.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}
	var err error
	if server.aeLoop, err = ae.AeLoopCreate(); err != nil {
		return err
	}
	server.fd, err = net.TcpServer(server.port)
	return err
}

func main() {
	path := os.Args[1]
	config, err := conf.LoadConfig(path)

	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}

	server.aeLoop.AddFileEvent(server.fd, ae.AE_READABLE, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(ae.AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.aeLoop.AeMain()
}
