package list

import (
	"akt-redis/obj"
	"akt-redis/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	list := ListCreate(ListType{EqualFunc: utils.GStrEqual})
	assert.Equal(t, list.Length(), 0)

	list.Append(obj.CreateObject(obj.GSTR, "4"))
	list.DelNode(list.First())

	list.Append(obj.CreateObject(obj.GSTR, "1"))
	list.Append(obj.CreateObject(obj.GSTR, "2"))
	list.Append(obj.CreateObject(obj.GSTR, "3"))
	assert.Equal(t, list.Length(), 3)
	assert.Equal(t, list.First().Val.Val_.(string), "1")
	assert.Equal(t, list.Last().Val.Val_.(string), "3")

	o := obj.CreateObject(obj.GSTR, "0")
	list.LPush(o)
	assert.Equal(t, list.Length(), 4)
	assert.Equal(t, list.First().Val.Val_.(string), "0")

	list.LPush(obj.CreateObject(obj.GSTR, "-1"))
	assert.Equal(t, list.Length(), 5)
	n := list.Find(o)
	assert.Equal(t, n.Val, o)

	list.Delete(o)
	assert.Equal(t, list.Length(), 4)
	n = list.Find(o)
	assert.Nil(t, n)

	list.DelNode(list.First())
	assert.Equal(t, list.Length(), 3)
	assert.Equal(t, list.First().Val.Val_.(string), "1")

	list.DelNode(list.Last())
	assert.Equal(t, list.Length(), 2)
	assert.Equal(t, list.Last().Val.Val_.(string), "2")
}
