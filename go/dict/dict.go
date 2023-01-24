package dict

import (
	"akt-redis/obj"
	"errors"
	"math"
	"math/rand"
)

const (
	INIT_SIZE    int64 = 8
	FORCE_RATIO  int64 = 2 // 强制扩容阈值
	GROW_RATIO   int64 = 2 // 扩容幅度
	DEFAULT_STEP int   = 1
)

var (
	EP_ERR = errors.New("expand error")
	EX_ERR = errors.New("key exists error")
	NK_ERR = errors.New("key doesnt exist error")
)

type Entry struct {
	Key  *obj.Gobj
	Val  *obj.Gobj
	next *Entry
}

type htable struct {
	table []*Entry
	size  int64
	mask  int64
	used  int64
}

type DictType struct {
	HashFunc  func(key *obj.Gobj) int64
	EqualFunc func(k1 *obj.Gobj, k2 *obj.Gobj) bool
}

type Dict struct {
	DictType
	hts       [2]*htable
	rehashidx int64
}

func DictCreate(distType DictType) *Dict {
	return &Dict{
		DictType:  distType,
		rehashidx: -1,
	}
}

func nextPower(size int64) int64 {
	num := INIT_SIZE
	for num < math.MaxInt64 {
		if num >= size {
			return num
		}
		num *= 2
	}
	return -1
}

func (dict *Dict) expand(size int64) error {
	// 找寻大于等于size的最近2**n
	num := nextPower(size)
	if dict.isRehashing() || (dict.hts[0] != nil && dict.hts[0].size >= num) {
		return EP_ERR
	}
	ht := htable{
		size:  num,
		mask:  num - 1,
		table: make([]*Entry, num),
		used:  0,
	}
	// 初始化时
	if dict.hts[0] == nil {
		dict.hts[0] = &ht
		return nil
	}
	// rehash
	dict.hts[1] = &ht
	dict.rehashidx = 0
	return nil
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashidx != -1
}

func (dict *Dict) expandIfNeeded() error {
	// rehash 中
	if dict.isRehashing() {
		return nil
	}
	// 刚初始化完毕
	if dict.hts[0] == nil {
		return dict.expand(INIT_SIZE) // redis 源码INIT_SIZE为4
	}
	// 当前数目/大小 > 强制扩容阈值，进行扩容
	hashTable := dict.hts[0]
	if (hashTable.used > hashTable.size) && (hashTable.used/hashTable.size > FORCE_RATIO) {
		return dict.expand(hashTable.size * GROW_RATIO)
	}
	return nil
}

func (dict *Dict) rehashStep() {
	dict.rehash(DEFAULT_STEP)
}

func (dict *Dict) rehash(step int) {
	// 由于单线程，redis将rehash拆分成为小步骤进行rehash，以保证不会因为table过大而引发block线程
	for step > 0 {
		// 已经rehash结束时
		if dict.hts[0].used == 0 {
			dict.hts[0] = dict.hts[1]
			dict.hts[1] = nil
			dict.rehashidx = -1
			return
		}
		// 寻找第一个非空值
		for dict.hts[0].table[dict.rehashidx] == nil {
			dict.rehashidx++
		}

		// 开始重新遍历非空值下hash，迁移至扩容后的table
		entry := dict.hts[0].table[dict.rehashidx]
		for entry != nil {
			ne := entry.next
			idx := dict.HashFunc(entry.Key) & dict.hts[1].mask
			entry.next = dict.hts[1].table[idx]
			dict.hts[1].table[idx] = entry
			dict.hts[0].used -= 1
			dict.hts[1].used += 1
			entry = ne
		}
		dict.hts[0].table[dict.rehashidx] = nil
		dict.rehashidx += 1
		step -= 1
	}
}

func freeEntry(e *Entry) {
	e.Key.DecrRefCount()
	e.Val.DecrRefCount()
}

func (dict *Dict) Delete(key *obj.Gobj) error {
	if dict.hts[0] == nil {
		return NK_ERR
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}

	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				if prev == nil {
					dict.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}
				freeEntry(e)
				return nil
			}
			prev = e
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return NK_ERR
}

func (dict *Dict) RandomGet() *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	t := 0
	if dict.isRehashing() {
		dict.rehashStep()
		if dict.hts[1] != nil && dict.hts[1].used > dict.hts[0].used {
			t = 1
		}
	}
	idx := rand.Int63n(dict.hts[t].size)
	cnt := 0
	for dict.hts[t].table[idx] == nil && cnt < 1000 {
		idx = rand.Int63n(dict.hts[t].size)
		cnt += 1
	}
	if dict.hts[t].table[idx] == nil {
		return nil
	}
	var listLen int64
	p := dict.hts[t].table[idx]
	for p != nil {
		listLen += 1
		p = p.next
	}
	listIdx := rand.Int63n(listLen)
	p = dict.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}
	return p
}

func (dict *Dict) keyIndex(key *obj.Gobj) int64 {
	err := dict.expandIfNeeded()
	if err != nil {
		return -1
	}
	h := dict.HashFunc(key)
	var idx int64
	for i := 0; i <= 1; i++ {
		idx = h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) AddRaw(key *obj.Gobj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// 获取index
	idx := dict.keyIndex(key)
	if idx == -1 {
		return nil
	}
	var ht *htable
	if dict.isRehashing() {
		ht = dict.hts[1]
	} else {
		ht = dict.hts[0]
	}
	var e Entry
	e.Key = key
	key.IncrRefCount()
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used += 1
	return &e
}

func (dict *Dict) Add(key *obj.Gobj, val *obj.Gobj) error {
	entry := dict.AddRaw(key)
	if entry == nil {
		return EX_ERR
	}
	entry.Val = val
	val.IncrRefCount()
	return nil
}

func (dict *Dict) Set(key *obj.Gobj, val *obj.Gobj) {
	if err := dict.Add(key, val); err == nil {
		return
	}
	entry := dict.Find(key)
	entry.Val.DecrRefCount()
	entry.Val = val
	val.IncrRefCount()
}

func (dict *Dict) Find(key *obj.Gobj) *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) Get(key *obj.Gobj) *obj.Gobj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.Val
}
