package main

import (
	"akt-redis/ae"
	"akt-redis/conf"
	"akt-redis/dict"
	"akt-redis/list"
	"akt-redis/net"
	"akt-redis/obj"
	"time"

	"hash/fnv"
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
	fd    int
	db    *GodisDB
	args  []*obj.Gobj
	reply *list.List
}

var server GodisServer

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

// 接受client
func AcceptHandler(loop *ae.AeLoop, fd int, extra interface{}) {
	cfd, err := net.Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}

	// TODO: CreateClient
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
