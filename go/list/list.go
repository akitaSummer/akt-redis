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
	Head   *Node
	Tail   *Node
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
	if list.Head == nil {
		list.Head = &node
		list.Tail = &node
	} else {
		node.prev = list.Tail
		list.Tail.next = &node
		list.Tail = &node
	}
	list.length += 1
}

func (list *List) DelNode(n *Node) {
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	return list.Head
}
