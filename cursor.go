package bolt

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
type Cursor struct {
	bucket *Bucket // 保存当前遍历的（指向的）bucket，一棵 b+ 树
	stack  []elemRef //	保存遍历搜索的路径，即按顺序缓存遍历过程中遇到的 node 或者 page，切片的最后一个元素为当前遍历的节点
}

// Bucket returns the bucket that this cursor was created from.
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
// First 即将游标定位到当前 bucket 的第一个 key 的位置。
// 先清空栈，然后返回 b+ 树的根节点，并追加到 stack 中，最后将游标移动到第一个叶子节点。
// 若发现该页为空页，即不包含任何 kv，则继续移动到下一个节点。
// 否则获取对应节点的 key 和 value
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.first()

	// If we land on an empty page then move to the next value.
	// https://github.com/boltdb/bolt/issues/450
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 { // 若为 subbucke 类型，则 value 为空。
		return k, nil
	}
	return k, v

}

// Last moves the cursor to the last item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
// Next 返回下一个元素
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each page in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil
	}

	// Move down the stack to find the last element of the last leaf under this branch.
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nil key is returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is not writable.
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	// Return an error if current value is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
// 从根节点开始搜索 key，层层往下搜索（每层都是用二分搜索），直到遇到叶子节点。
// 当遇到叶子节点，则继续基于 nsearch 函数使用二分搜索查找指定的 key，
// 若找到了，则返回对应的 key 和 value，否则若未找到则返回下一个元素的 key 和 value，即定位到的是 key 应当插入的点
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Start from root page/node and traverse to correct page.
	// 先清空查找栈，然后从根节点开始查找指定 key
	c.stack = c.stack[:0]
	c.search(seek, c.bucket.root)
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	// 栈顶元素的 index 值即为目标节点的 inodes 数组中指定 key 所在的 index，或者应该存在的（第一个大于该 key 的位置）index
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	// If this is a bucket then return a nil value.
	// 返回栈顶元素的第 index 个 kv 对（同样是先在 node 中查找，再去 page 中查找）
	return c.keyValue()
}

// first moves the cursor to the first leaf element under the last page in the stack.
// first 函数不断搜索每一个分支节点的第一个孩子节点（index=0）直至搜索到叶子节点，则退出。
// 此时 stack 的顶部即为目标叶子节点
func (c *Cursor) first() {
	for {
		// Exit when we hit a leaf page.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the first element to the stack.
		// 不断地将第一个孩子分支节点入栈
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// last moves the cursor to the last leaf element under the last page in the stack.
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf page.
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the last element in the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
// next 将游标移动到下一个叶子节点，并返回 key 和 value，否则返回空。
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		// 自下而上地查找，直到找到一个可以递增的节点。
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 { // 该节点必须可以递增。这里通过自增来达到访问下一个元素的目的。
				elem.index++
				break
			}
		}

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		// 直到根节点也仍旧未找到支持递增的节点，则返回空
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		// 更新当前节点
		c.stack = c.stack[:i+1]
		c.first() // 从当前节点开始向下搜索直到遇到叶子节点

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		// 若是空的叶子节点，则继续找。否则，返回该叶子结点的 key 和 value
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// search recursively performs a binary search against a given page/node until it finds a given key.
// search 递归地执行一个二分查找，直到找到指定的 key
// 注意：如果 key 不存在时，search 定位到的是 key 应当插入的点，即第一个大于指定 key 的点
func (c *Cursor) search(key []byte, pgid pgid) {
	// 获取当前查找的指定 page id 的 node 和 page
	p, n := c.bucket.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	// 将当前查找的节点入栈
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf page/node then find the specific node.
	// 若当前节点为叶子节点，则在叶子节点包含的 kv 集合中，继续以二分的方式进行查找指定 key
	// 若查找成功，则设置栈顶元素的 index
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	// 否则即为分支节点。则先在 node 中递归查找，但若 node 未存在（即未加载到内存）则再去 page 中递归查找。
	if n != nil {
		c.searchNode(key, n)
		return
	}
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	// 在 node 的 inodes 中查找，返回 key 对应的 index，或者在未找到时返回最后一个小于指定 key 的下标。
	// 这里之所以要返回第一个小于指定 key 的 index，是因为分支节点保存的 key 为其指向的节点的 key，也即其节点所包含元素的最小 key。
	// 因此不能去大于该 key 的范围查找（肯定找不到），而是要到小于目标 key 的最大的 index 去找，才可能找到等于目标 key 的元素。
	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	// 在未精确匹配的情况下，本来返回的是第一个大于目标 key 的 index，这里进行了调整
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	// 然后继续在指定的 page 中查找
	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, inodes[index].pgid)
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// If we have a node then search its inodes.
	// 优先从 node 中查找其 inodes 数组，找到一个 key 相等或者第一个更大的 index，
	// 其被设置为当前 cursor 的 index，表示当前栈的栈顶的节点中的第 index 个元素即为目标查找元素
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	// 若 node 为空，则从 page 中查找
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// keyValue returns the key and value of the current leaf element.
// keyValue 返回当前搜索栈的栈顶元素（叶子节点）的 key 和 value 以及 flag 标志
// 同样是先去 node 中查找再去 page 中查找
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// Retrieve value from node.
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// Or retrieve value from page.
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// node returns the node that the cursor is currently positioned on.
// node 返回 cursor 当前指向的 node
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	// 若栈顶的元素为 leaf node，则直接返回它
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// Start from root and traverse down the hierarchy.
	// 否则只能通过从 root 开始，层层往下遍历来找到当前指向的叶子节点的元素。
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		// 获取第 index 个孩子节点，在该函数中会将 cursor 栈中保存的所有节点索引加载为 node，并缓存起来，避免重复加载以进行复用
		n = n.childAt(int(ref.index))
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given page/node.
// elemRef 在遍历 b+ 树节点使用，记录遍历过程中经过的每一个节点。
// 其中 page 和 node 是一一对应的，如果 page 加载到了内存中（通过 page 转换而来），则优先使用 node，否则使用 page；
// 而 index 表示路径经过该节点时在其父节点中的 inodes 中的索引
type elemRef struct {
	page  *page
	node  *node
	index int // index 表示在下一层要搜索数据时的目标孩子节点的索引
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
