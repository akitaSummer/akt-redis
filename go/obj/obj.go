package obj

import "strconv"

type Gtype uint8

type Gval interface{}

const (
	// string
	GSTR Gtype = 0x00
	// list
	GLIST Gtype = 0x01
	// set
	GSET Gtype = 0x02
	// zset
	GZSET Gtype = 0x03
	// dist
	GDICT Gtype = 0x04
)

type Gobj struct {
	Type_    Gtype
	Val_     Gval
	refCount int
}

func (obj *Gobj) IntVal() int64 {
	if obj.Type_ != GSTR {
		return 0
	}
	val, _ := strconv.ParseInt(obj.Val_.(string), 10, 64)
	return val
}

func (o *Gobj) StrVal() string {
	if o.Type_ != GSTR {
		return ""
	}
	return o.Val_.(string)
}

func CreateFromInt(val int64) *Gobj {
	return &Gobj{
		Type_:    GSTR,
		Val_:     strconv.FormatInt(val, 10),
		refCount: 1,
	}
}

func CreateObject(typ Gtype, ptr interface{}) *Gobj {
	return &Gobj{
		Type_:    typ,
		Val_:     ptr,
		refCount: 1,
	}
}

func (o *Gobj) IncrRefCount() {
	o.refCount++
}

func (o *Gobj) DecrRefCount() {
	o.refCount--
	if o.refCount == 0 {
		// 垃圾回收自动回收Val_
		o.Val_ = nil
	}
}
