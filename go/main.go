package main

import (
	"akt-redis/ae"
	"akt-redis/conf"
	"akt-redis/dict"
	"akt-redis/list"
	"akt-redis/net"
	"akt-redis/obj"
	"akt-redis/utils"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"log"
	"os"
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
	queryLen int // client.queryLen: 未处理的长度
	cmdType  utils.CmdType
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
	if index < 0 && client.queryLen > utils.GODIS_MAX_INLINE {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

var server GodisServer

var cmdTable []GodisCommand = []GodisCommand{
	{"get", getCommand, 2},
	{"set", setCommand, 3},
	{"expire", expireCommand, 3},
}

// resp 协议返回值，返回给client
// 通过首字符和结尾 "\r\n" 区分每一条命令
// +: 简单string
// -: error 如错误返回
// :: int 如自增命令后的返回
// $: string，"$4\r\ntest\r\n"后接数字，告诉string长度，\r\n后为string文本，可以包含换行符之类的字符
// *: array 后接数字，表示数组长度

func expireIfNeeded(key *obj.Gobj) {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > utils.GetMsTime() {
		return
	}
	server.db.expire.Delete(key)
	server.db.data.Delete(key)
}

func findKeyRead(key *obj.Gobj) *obj.Gobj {
	expireIfNeeded(key)
	return server.db.data.Get(key)
}

func getCommand(c *GodisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$-1\r\n")
	} else if val.Type_ != obj.GSTR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(client *GodisClient) {
	key := client.args[1]
	val := client.args[2]

	if val.Type_ != obj.GSTR {
		client.AddReplyStr("-ERR: wrong type\r\n")
	}

	server.db.data.Set(key, val)
	server.db.expire.Delete(key)
	client.AddReplyStr("+OK\r\n")
}

func expireCommand(c *GodisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != obj.GSTR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	expire := utils.GetMsTime() + (val.IntVal() * 1000)
	expObj := obj.CreateFromInt(expire)
	server.db.expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK\r\n")
}

func CreateClient(fd int) *GodisClient {
	var client GodisClient
	client.fd = fd
	client.db = server.db
	client.queryBuf = make([]byte, utils.GODIS_IO_BUF)
	client.reply = list.ListCreate(list.ListType{EqualFunc: utils.GStrEqual})
	return &client
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdType = utils.COMMAND_UNKNOWN
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
	// init bulkNum
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}

		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*obj.Gobj, bnum)
	}
	// 读取所有 bulk string
	for client.bulkNum > 0 {
		// init bulkLen
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}

			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}

			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > utils.GODIS_MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		// read bulk string
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		// 判断结尾是否为\r\n
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		client.args[len(client.args)-client.bulkNum] = obj.CreateObject(obj.GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}

	return true, nil
}

// redis 命令
// InLine: "set key val\r\n"
// MuttiBulk: "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"

// 处理client query
func ProcessQueryBuf(client *GodisClient) error {
	// 不断取值
	for len(client.queryBuf) > 0 {
		if client.cmdType == utils.COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdType = utils.COMMAND_BULK
			} else {
				client.cmdType = utils.COMMAND_INLINE
			}
		}

		// 读取内容转化为cmd args
		var ok bool
		var err error
		if client.cmdType == utils.COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdType == utils.COMMAND_BULK {
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
			// 未读取完，下次再处理
			break
		}
	}
	return nil
}

func ReadQueryFromClient(loop *ae.AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	// 如果剩余大小不足GODIS_MAX_BULK，则扩容
	if len(client.queryBuf)-client.queryLen < utils.GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, utils.GODIS_MAX_BULK)...)
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
		data:   dict.DictCreate(dict.DictType{HashFunc: utils.GStrHash, EqualFunc: utils.GStrEqual}),
		expire: dict.DictCreate(dict.DictType{HashFunc: utils.GStrHash, EqualFunc: utils.GStrEqual}),
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
