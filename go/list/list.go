package list

import "akt-redis/obj"

// 双向链表

type Node struct {
	Val  *obj.Gobj
	next *Node
	prev *Node
}

type ListType struct {
	EqualFunc func(a, b *obj.Gobj) bool
}

type List struct {
	ListType
	head   *Node
	tail   *Node
	length int
}

func ListCreate(listType ListType) *List {
	return &List{
		ListType: listType,
	}
}

func (list *List) Append(val *obj.Gobj) {
	node := Node{
		Val: val,
	}
	if list.head == nil {
		list.head = &node
		list.tail = &node
	} else {
		node.prev = list.tail
		list.tail.next = &node
		list.tail = &node
	}
	list.length += 1
}
