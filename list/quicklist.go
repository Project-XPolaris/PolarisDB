package list

type QuickList struct {
	head *Node
}

type Node struct {
	prev *Node
	next *Node
	zl   []byte
}

func NewQuickList() *QuickList {
	return &QuickList{}
}

func (q *QuickList) Len() int {
	it := q.GetIterator()
	count := 0
	for {
		if it.Next() == nil {
			break
		}
		count++
	}
	return count
}
func (q *QuickList) InsertAt(pos int, data []byte) {
	// if quicklist is empty
	if q.head == nil {
		newZl := NewZiplistEntry()
		newZl.Insert(0, data)
		q.head = &Node{
			zl: newZl.Encode(),
		}
		q.head.next = q.head
		q.head.prev = q.head
		return
	}
	// find the node to insert
	count := 0
	cur := q.head
	var targetZipList *ziplistEntry
	for ; cur != nil; cur = cur.next {
		targetZipList = cur.Decode()
		targetElmCount := targetZipList.Count()
		if count+targetElmCount >= pos {
			break
		}
		count += targetElmCount
	}
	if targetZipList.CanInsert(data) {
		// split
		targetZipList.Insert(pos-count, data)
		cur.zl = targetZipList.Encode()
	} else {

		// target ---> new ---> back
		if pos-count == 0 {
			// insert to next node
			if cur.next.Decode().CanInsert(data) {
				// lpush to next node
				cur.next.Decode().Insert(0, data)
			} else {
				newZl := NewZiplistEntry()
				newZl.Insert(0, data)
				newNode := &Node{
					zl: newZl.Encode(),
				}
				newNode.next = cur.next
				newNode.prev = cur
				cur.next = newNode
				if cur == q.head {
					// insert to head
					q.head = newNode
				}
			}
			return
		}
		// must split
		newZiplist := targetZipList.SplitAt(pos - count)
		newZiplist.Insert(0, data)
		newNode := &Node{
			zl:   newZiplist.Encode(),
			next: cur.next,
			prev: cur,
		}
		cur.zl = targetZipList.Encode()
		cur.next.prev = newNode
		cur.next = newNode

	}
	// find tail
	//tail := q.head.prev
	//newNode := &Node{}
	//newNode.Insert(data)
	//newNode.prev = tail
	//newNode.next = q.head
	//tail.next = newNode
	//q.head.prev = newNode
}
func (q *QuickList) DeleteAt(index int) {
	// find the node to delete
	count := 0
	cur := q.head
	var targetZipList *ziplistEntry
	for ; cur != nil; cur = cur.next {
		targetZipList = cur.Decode()
		targetElmCount := targetZipList.Count()
		if count+targetElmCount >= index {
			break
		}
		count += targetElmCount
	}
	targetZipList.Delete(index - count)
	cur.zl = targetZipList.Encode()
}
func (q *QuickList) Index(index int) []byte {
	count := 0
	cur := q.head
	var targetZipList *ziplistEntry
	for ; cur != nil; cur = cur.next {
		targetZipList = cur.Decode()
		targetElmCount := targetZipList.Count()
		if count+targetElmCount >= index {
			break
		}
		count += targetElmCount
	}
	return targetZipList.Index(index - count).data
}
func (q *QuickList) Range(start, end int) [][]byte {
	res := make([][]byte, 0)
	it := q.GetIterator()
	count := 0
	for {
		data := it.Next()
		if data == nil {
			break
		}
		if count >= start && count <= end {
			res = append(res, data)
		}
		count++
	}
	return res
}
func (q *Node) Decode() *ziplistEntry {
	zl := NewZiplistEntry()
	zl.Decode(q.zl)
	return zl
}

type QuickListIterator struct {
	// current position
	pos        int
	curNode    *Node
	curZiplist *ziplistEntry
	curIter    *iterator
	QuickList  *QuickList
}

func (q *QuickList) GetIterator() *QuickListIterator {
	return &QuickListIterator{
		pos:       -1,
		QuickList: q,
	}
}
func (it *QuickListIterator) Next() []byte {
	if it.QuickList.head == nil {
		return nil
	}
	if it.curNode == nil {
		it.curNode = it.QuickList.head
		it.curZiplist = NewZiplistEntry()
		it.curZiplist.Decode(it.curNode.zl)
		it.curIter = it.curZiplist.GetIterator()
	}
	nextEntry := it.curIter.Next()
	if nextEntry == nil {
		// change to next node
		it.curNode = it.curNode.next
		if it.curNode == it.QuickList.head {
			return nil
		}
		it.curZiplist = NewZiplistEntry()
		it.curZiplist.Decode(it.curNode.zl)
		it.curIter = it.curZiplist.GetIterator()
		nextEntry = it.curIter.Next()
		return nextEntry.data
	}
	return nextEntry.data
}
