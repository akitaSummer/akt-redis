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
	if n == nil {
		return
	}
	if n == list.Head {
		if n.next != nil {
			n.next.prev = nil
		}
		list.Head = n.next
		n.next = nil
	} else if n == list.Tail {
		if n.prev != nil {
			n.prev.next = nil
		}
		list.Tail = n.prev
		n.prev = nil
	} else {
		if n.prev != nil {
			n.prev.next = n.next
		}
		if n.next != nil {
			n.next.prev = n.prev
		}
		n.prev = nil
		n.next = nil
	}
	list.length -= 1
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	return list.Head
}

func (list *List) Last() *Node {
	return list.Tail
}

func (list *List) Delete(val *obj.Gobj) {
	list.DelNode(list.Find(val))
}

func (list *List) Find(val *obj.Gobj) *Node {
	p := list.Head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			return p
		}
		p = p.next
	}
	return nil
}

func (list *List) LPush(val *obj.Gobj) {
	node := Node{
		Val:  val,
		next: list.Head,
		prev: nil,
	}
	if list.Head != nil {
		list.Head.prev = &node
	} else {
		list.Tail = &node

	}

	list.Head = &node
	list.length += 1
}
