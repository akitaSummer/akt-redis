package dict

import "akt-redis/obj"

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

func (dict *Dict) Delete(key *obj.Gobj) error {
	return nil
}

func (dict *Dict) RandomGet() *Entry {
	return nil
}

func (dict *Dict) RemoveKey(key *obj.Gobj) {
}

func (dict *Dict) Set(key *obj.Gobj, val *obj.Gobj) {
}

func (dict *Dict) Find(key *obj.Gobj) *Entry {
	return nil
}

func (dict *Dict) Get(key *obj.Gobj) *obj.Gobj {
	return nil
}
