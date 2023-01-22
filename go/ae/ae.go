package ae

import (
	"log"
	"time"

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

var fe2ep = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}

type AeFileEvent struct {
	fd    int
	mask  FeType
	cb    FileCallback
	extra interface{}
}

type AeTimeEvent struct {
	id       int
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

// 返回fileEvent的key，可读为正，可写为负
func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	} else {
		return fd * -1
	}
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
	for !loop.stop {
		tes, fes := loop.AeWait()
		loop.AeProcess(tes, fes)
	}
}

// 获取最近的时间
func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

// 等待事件
func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	// 获取超时时间
	timeout := loop.nearestTime() - GetMsTime()
	if timeout <= 0 {
		timeout = 10 // at least wait 10ms
	}
	var events [128]unix.EpollEvent
	// 获取两次timeout之间的所有fe事件
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		log.Printf("epoll wait warnning: %v\n", err)
	}
	if n > 0 {
		log.Printf("ae get %v epoll events\n", n)
	}
	// 查询对应事件，并存储到fes中
	for i := 0; i < n; i++ {
		if events[i].Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_READABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if events[i].Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_WRITABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}
	// 查询需要执行的事件
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return nil, nil
}

// 处理事件
func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.cb(loop, te.id, te.extra)
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = GetMsTime() + te.interval
		}
	}

	for _, fe := range fes {
		fe.cb(loop, fe.fd, fe.extra)
	}
}

func (loop *AeLoop) getEpollMask(fd int) (ev uint32) {
	if loop.FileEvents[getFeKey(fd, AE_READABLE)] != nil {
		ev |= fe2ep[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, AE_WRITABLE)] != nil {
		ev |= fe2ep[AE_WRITABLE]
	}
	return
}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, callback FileCallback, extra interface{}) {
	// 查询当前fd是否存在可读或可写事件
	ev := loop.getEpollMask(fd)
	// ev != 0 && fe2ep[mask] != 0
	if ev&fe2ep[mask] != 0 {
		return
	}
	op := unix.EPOLL_CTL_ADD
	// 没有可读或可写事件
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}

	ev |= fe2ep[mask]
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll ctr err: %v\n", err)
		return
	}

	loop.FileEvents[getFeKey(fd, mask)] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		cb:    callback,
		extra: extra,
	}
	log.Printf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= ^fe2ep[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll del err: %v\n", err)
	}

	loop.FileEvents[getFeKey(fd, mask)] = nil
	log.Printf("ae remove file event fd:%v, mask:%v\n", fd, mask)
}

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, callback TimeCallback, extra interface{}) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	loop.TimeEvents = &AeTimeEvent{
		id:       id,
		mask:     mask,
		interval: interval,
		when:     GetMsTime() + interval,
		cb:       callback,
		extra:    extra,
		next:     loop.TimeEvents,
	}
	return id
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
				p.next = nil
			}
			break
		}
		pre = p
		p = p.next
	}
}
