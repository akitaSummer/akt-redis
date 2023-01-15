package ae

import (
	"golang.org/x/sys/unix"
)

type FeType int

const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2
)

type TeType int

const (
	AE_NORMAL TeType = 1
	AE_ONCE   TeType = 2
)

type FileCallback func(loop *AeLoop, fd int, extra interface{})
type TimeCallback func(loop *AeLoop, fd int, extra interface{})

type AeFileEvent struct {
	fd    int
	mask  FeType
	cb    FileCallback
	extra interface{}
}

type AeTimeEvent struct {
	fd       int
	mask     TeType
	when     int64 //ms
	interval int64 // 频率
	cb       TimeCallback
	extra    interface{}
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

func AeLoopCreate() (*AeLoop, error) {
	// mac 未暴露
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
	}, nil
}

// 主循环
func (loop *AeLoop) AeMain() {

}

// 等待事件
func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	return nil, nil
}

// 处理事件
func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {

}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, callback FileCallback, extra interface{}) {
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, callback TimeCallback, extra interface{}) int {
	return 0
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
}
