package utils

import (
	"akt-redis/obj"
	"hash/fnv"
	"time"
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

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
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
