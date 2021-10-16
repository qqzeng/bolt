package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
// node 表示 page 在内存中的表示。在内存中，具体的一个分支节点或者叶子节点都被抽象为一个 node 对象
// page 和 node 的对应关系为：文件系统中一组连续的物理 page，加载到内存（mmap 操作）成为一个逻辑 page ，进而转化为（node.read）一个 node
type node struct {
	bucket     *Bucket // node 所属的 bucket
	isLeaf     bool // 该 node 表示的是叶子节点还是分支节点
	// 只有 node.del() 的调用才会导致 n.unbalanced 被标记，且只有用户在某次写事务中删除数据时，才会引起 node.rebalance 的执行
	unbalanced bool // 该 node 是否平衡，当 node 存储的 key 发生变化时会被设置为不平衡
	spilled    bool // 该 node 包含的数据是否已被刷新到脏页，在事务提交时，其会被拆分和落盘，然后设置该值为 true
	key        []byte // 第一个子节点的 key，因此，对于叶子节点该值为空
	pgid       pgid // 该 node 对应的 page 的 page id
	parent     *node // 父节点指针
	children   nodes // 孩子节点集合指针（只包含加载到内存中的部分孩子），用于 spill 操作时先 spill 子节点，因此叶子节点该值为空
	inodes     inodes // 该 node 包含的 kv 数据，对于分支节点是 key+pgid 数组，对于叶子节点是 kv 数组
}

// root returns the top-level node this node is attached to.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the size of the node after serialization.
// size 返回该 node 序列化后的大小：page 头 + 元信息数组大小 + 实际的 kv 数据大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

// pageElementSize returns the size of each page element based on the type of node.
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
// childIndex 基于二分查找到第一个 key 大于指定 key 的元素的索引。
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
// nextSibling 返回下一个兄弟节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
// nextSibling 返回上一个兄弟节点
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.
// put 插入一个 key value
// 若插入的是一个 key/value（包括 leaf bucket），需要指定 pgid 为 0。
// 相反，若 put 的一个分支节点，则需要指定 pgid，但不需要指定 value
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	// 通过二分查找插入的合适的位置，第一个大于或等于 key 的索引
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	// 判断此插入操作是否为新增，即判断此次插入的 key 和返回的索引位置的 key 是否相等，
	// 若不等，则将目标放置的索引后的序列整体后移一位，然后将 index 处设置为当前 put 的 kv
	if !exact {
		n.inodes = append(n.inodes, inode{}) // 需要提前扩大 inodes 数组
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	// 反之若相等，则直接覆盖设置 inode 各个属性值
	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
// del 操作比较简单，即使用二分查找当前 key 对应的 inode，若未找到，则直接返回。
// 否则，将当前 inode 之后的序列前移覆盖当前元素，最后标记当前 node 为 unbalanced，以触发 rebalance 操作
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// read initializes the node from a page.
// read 根据一个 page 来初始化一个 node
func (n *node) read(p *page) {
	n.pgid = p.id // 设置 page id
	n.isLeaf = ((p.flags & leafPageFlag) != 0) // 设置 node 类型，为分支节点还是叶子节点类型
	n.inodes = make(inodes, int(p.count)) // 根据 page 的实际类型包含的 page 数目（分支节点）或者 kv 数目（叶子节点）来创建对一个的 inodes 数组。

	for i := 0; i < int(p.count); i++ {
		// 获取第 i 个 node，后续为其赋值
		inode := &n.inodes[i]
		// 若为叶子节点，则获取 page 的第 i 个叶子节点元素（元信息）
		// 依次设置 inode 的 flags、key 和 value
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			// 否则若为分支节点，则同样获取第 i 个分支节点元素（元信息）
			// 依次设置 inode 的 flags（为 false）、page id 以及 key
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// Save first key so we can find the node in the parent when we spill.
	// 保存首个 key 元素，便于在 spill 的时候能够在父节点中顺利定位到该 node TODO【1】
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key // key 表示 inodes 数组中的首个元素的 key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
// write 将 node 写入到 page 中
func (n *node) write(p *page) {
	// Initialize page.
	// 判断为叶子节点还是分支节点
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	// 这里对于叶子节点不可能溢出，因为溢出时（超过 0xFFFF 个 kv 数据）会进行分裂
	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// Stop here if there are no items to write.
	if p.count == 0 {
		return
	}

	// Loop over each item and write it to the page.
	// 跳过了 page 的 element（元信息）部分，b 就指向的是 page 的 body 起始位置
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Write the page element.
		// 写入叶子节点的元信息或者分支节点元信息
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem))) // pos 表示该 ele 距离 body 中该 key 的实际距离
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// If the length of key+value is larger than the max allocation size
		// then we need to reallocate the byte array pointer.
		//
		// See: https://github.com/boltdb/bolt/pull/335
		klen, vlen := len(item.key), len(item.value)
		// 若 key 和 value 的大小之和大于分配的最大大小，则需要重新追加分配一个 maxAllocSize 大小的数组
		if len(b) < klen+vlen {
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// Write data for the element to the end of the page.
		// 然后再依次写入 key 和 value，每次写完都会修改 b 的偏移量
		copy(b[0:], item.key)
		b = b[klen:]
		copy(b[0:], item.value)
		b = b[vlen:]
	}

	// DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
// spilt 将当前节点分裂为若干个更小的节点，
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		// 调用 splitTwo 将节点一分为二，其中 a 为符合要求的节点，b 为可能将被继续分裂的节点
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		// 知道已经不能再进行分裂了，退出
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		// 继续分裂后续部分
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
// splitTwo 将当前 node 分裂为两个 node，保证第一个 node 的大小不超过页大小
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	// 不满足分裂条件（key 数目过少，小于4，或者节点大小过小，小于 pageSize），则不需分裂，直接将 b 设置为 nil，返回
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	// 确定 inodes 子节点数组将被分割的索引位置
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	// 如当前节点的父节点不存在，则创建一个
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	// 创建一个分裂出来的节点，并将其加入到当前节点的父节点的孩子节点集中。
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	// 将当前节点的子节点分为两个集合，分别作为当前节点的剩余子节点集合以及分裂出来的节点的子节点集合
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	n.bucket.tx.stats.Split++

	return n, next
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
// splitIndex 将一个节点一分为二，其中第一个节点的大小为不高于分裂阈值的大小，而不管剩下部分节点的大小。
// TODO 该种策略会不会导致不久的将来节点又很快会被分裂（因为其目前的大小已经临界分裂的阈值了）
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		// 分裂必须保证当前节点包含的 key 的数量不低于最小值，同时，当前节点的大小又高于分裂的阈值
		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
// spill 将 node 进行拆分并写入到脏页
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled { // 若已刷，则直接返回。
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	// 自底向上，先刷子节点
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	// 已刷，清空
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	// 先对当前 node 执行分裂操作，再依次将分裂后的 node 刷到脏页。
	var nodes = n.split(tx.db.pageSize)
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		// 将 node 占用的 page 回收放到 freelist 以后续复用
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// Allocate contiguous space for the node.
		// 重新分配连续的 page 来承载该 node 的数组
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// Write the node.
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		// 将 node 的数据刷到 page 上，并标记已刷。注意这里的 page 并不是真正的磁盘，真正刷到磁盘是在事务提交时进行
		node.pgid = p.id
		node.write(p)
		node.spilled = true // 标记已刷

		// Insert into parent inodes.
		// TODO 将当前节点的第一个子节点插入到当前节点的父节点的 inode 中。node.key 保存了该 node 的第一个子节点的 key
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// Update the statistics.
		tx.stats.Spill++
	}

	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	// 若当前节点的 root node 也被 split 了，同时创建了一个新的 root node，则同样需要考虑是否需要再 split 该 parent node。
	// 但为了避免重复调整，会将本节点 children 置空
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
// rebalance 表示 b+ 树相邻节点由于节点的填充率低于阈值（或者所包含的 key 数量过少）所进行的合并操作
func (n *node) rebalance() {
	if !n.unbalanced { // 若节点已平衡（即可能已执行过 rebalance 的操作），则直接返回，不需要再执行
		return
	}
	n.unbalanced = false // 设置为 false，表示已执行过 rebalance 操作

	// Update statistics.
	n.bucket.tx.stats.Rebalance++

	// Ignore if node is above threshold (25%) and has enough keys.
	// 若 node 占据的大小超过页（填充率）的 1/4 并且包含的 key 的数量超过 1 个叶子节点或者 2 个分支节点，则不需要进行 rebalance 操作。
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	// 否则，不满足上述条件且该 node 为 root node，即 b+ 树根节点，则需要特殊处理。
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		// 若该根节点为分支节点，且只包含一个子节点，则将该子节点提升为根节点
		// 即将该子节点的内容替换到父节点中，包括子节点的类型、包含的 kv 数组以及子节点数组。
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move root's child up.
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			// 调增子节点的子节点的父节点为当前节点
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// If node has no keys then just remove it.
	// 否则，也不为根节点，则若节点没有包含任何子节点，则直接移除它，同时递归地 rebalance 其父节点
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	// 当上述条件都不满足时，则根据当前节点为父节点的左孩子还是右孩子分别同右孩子或者左孩子进行合并
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		// 同右兄弟节点合并
		target = n.nextSibling()
	} else { // 同左兄弟节点合并
		target = n.prevSibling()
	}

	// If both this node and the target node are too small then merge them.
	// 若该节点和需要被合并的节点的填充率都过小，则将他们合并，最后删除被合并的节点
	if useNextSibling {
		// Reparent all child nodes being moved.
		// 同右兄弟节点合并，将继承右兄弟节点的所有孩子节点
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes from target and remove target.
		// 继承右兄弟节点的所有数据
		n.inodes = append(n.inodes, target.inodes...)
		// 将右兄弟从父节点移除
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		// 最后 free 掉右节点，即将右兄弟节点占据的 page id 归还给 freelist（的 pending 集合）
		target.free()
	} else {
		// Reparent all child nodes being moved.
		// 执行一个相反的过程
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	// 无论是该节点被合并（然后移除）还是其兄弟节点被合并（然后移除）都需要 rebalance 父节点。
	// 因为调整父节点包含的子节点的分布后（比如可能删除子节点），则父节点也需要递归调整
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
// dereference 在重新 mmap 时，使得 node 将其 key 以及 inode key/value 的引用拷贝到堆内存中，不至于 node 的数据仍然指向无效的内存地址。
func (n *node) dereference() {
	if n.key != nil { // 拷贝 key
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	// 拷贝 n.inodes 的 key 和 value
	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	// 递归地处理子节点
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	n.bucket.tx.stats.NodeDeref++
}

// free adds the node's underlying page to the freelist.
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

type nodes []*node

func (s nodes) Len() int           { return len(s) }
func (s nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool { return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1 }

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
// inode 表示 node 所含的内部元素，分支节点和叶子节点复用同一结构。
// 对于分支节点，单个元素为 key+引用（pgid）；对于叶子节点，单个元素为用户 kv 数据。
// 注意这里的分支节点的引用的类型为 pgid ，而非 node*。这是因为 inode 中指向的元素并不一定加载到了内存。
// boltdb 在访问 B+ 树时，会按需将访问到的 page 转化为 node，并将其指针存在父节点的 children 字段
type inode struct {
	flags uint32 // 用于区分 subbucket 和普通的 value
	pgid  pgid // 若表示分支节点，则代表分支节点元素所在的 b+ 树中下一层的 叶子节点或者分支节点的 page id，只有分支节点才有值
	key   []byte // 分支节点和叶子节点元素的 key
	value []byte // 叶子元素包含的 key 对应 value 值，且只有叶子节点才有值
}

type inodes []inode
