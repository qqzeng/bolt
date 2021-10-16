package bolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5 // 默认的填充率，当 node 的填充率高于该值时，则需要进行分裂

// Bucket represents a collection of key/value pairs inside the database.
// Bucket 表示数据库中 key/value 对的集合
type Bucket struct {
	// 内嵌其在磁盘上的 bucket
	*bucket
	// 该 bucket 关联的事务
	tx       *Tx                // the associated transaction
	// 该 bucket 的 subbucket 缓存
	buckets  map[string]*Bucket // subbucket cache
	// 当该 bucket 被内联时，其关联的内联页引用
	page     *page              // inline page reference
	// 该 bucket 的 root node
	rootNode *node              // materialized node for the root page.
	// 该 bucket 的 node 缓存
	nodes    map[pgid]*node     // node cache

	// Sets the threshold for filling nodes when they split. By default,
	// the bucket will fill to 50% but it can be useful to increase this
	// amount if you know that your write workloads are mostly append-only.
	//
	// This is non-persisted across transactions so it must be set in every Tx.
	// bucket 的填充率，同 split 和 merge 操作相关
	FillPercent float64
}

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
// bucket 表示一个 bucket 在磁盘上的结构
// 注意如果一个 bucket 内容过少，就会直接内嵌在父 bucket 的叶子节点中。
type bucket struct {
	root     pgid   // page id of the bucket's root-level page
	sequence uint64 // monotonically incrementing, used by NextSequence()
}

// newBucket returns a new bucket associated with a transaction.
// 创建一个 bucket 时，必须指定其关联的事务
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root returns the root of the bucket.
func (b *Bucket) Root() pgid {
	return b.root
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
// Cursor 创建一个关联指定 bucket 的游标，该游标可用于遍历 b+ 树查找居于指定 key 的 node
func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	b.tx.stats.CursorCount++

	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
// Bucket 返回具有指定 key 的 subbucket
func (b *Bucket) Bucket(name []byte) *Bucket {
	// 若检索的目标 subbucket 在 当前 bucket 的子 bucket 缓存中，则直接返回
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// Move cursor to key.
	// 否则，通过 cursor 来定位该 key 所对应的 bucket
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// Return nil if the key doesn't exist or it is not a bucket.
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// Otherwise create a bucket and cache it.
	// 基于检索出来的 value 创建一个 Bucket，并将其加入到父 bucket 的 subbucket 缓存中
	var child = b.openBucket(v)
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// If unaligned load/stores are broken on this arch and value is
	// unaligned simply clone to an aligned byte array.
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	if unaligned {
		value = cloneBytes(value)
	}

	// If this is a writable transaction then we need to copy the bucket entry.
	// Read-only transactions can point directly at the mmap entry.
	if b.tx.writable && !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// Save a reference to the inline page if the bucket is inline.
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
// CreateBucket 创建一个具有指定 key 的 subbucket
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	// 验证 db 的事务是否关闭，当前事务是否为读写事务，key 是否合法等
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// Move cursor to correct position.
	// 获取 cursor，并通过层层二分搜索定位到 key 所在位置，或者第一个大于 key 所在的位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key.
	// 若目标 key 已存在，即，
	// 若目标定位位置的 key 同当前 key 相同，且目标 key 所在的页为 subbucket，则抛出目标 bucket 已存在的异常。
	// 若目标 key 所在页为 leaf 页，则报页类型不兼容的错误
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	// Create empty, inline bucket.
	// 反之，目标 key 未找到，则创建一个 leaf bucket，并写入其 bucket 头及其 root page
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// Insert into node.
	// 插入 key 和 value。
	// 其中，pgid 为 0 表示其为内联 bucket
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// Since subbuckets are not allowed on inline buckets, we need to
	// dereference the inline page, if it exists. This will cause the bucket
	// to be treated as a regular, non-inline bucket for the rest of the tx.
	// 将当前 bucket 设置为非内联 bucket，因为其已经包含一个 subbucket
	b.page = nil

	// 返回父 bucket 的指定 key 值的 subbucket
	return b.Bucket(key), nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if bucket doesn't exist or is not a bucket.
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// Recursively delete all child buckets.
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove cached copy.
	delete(b.buckets, string(key))

	// Release all bucket pages to freelist.
	child.nodes = nil
	child.rootNode = nil
	child.free()

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	// Return nil if this is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
// Put 在指定的 bucket 中插入（覆盖） key 和 value 对
func (b *Bucket) Put(key []byte, value []byte) error {
	// 校验 db 关联的事务是否打开，事务是否为读写事务，key 和 value 的大小是否合法
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	// 类似地，获取 cursor，并通过层层二分搜索定位到 key 所在位置，或者第一个大于 key 所在的位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	// 若该 key 已存在，并且关联的 value 为 leaf bucket，则返回类型不兼容的错误
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	// 否则，插入 key 和 value，flags = 0 表示插入的使 leaf page。pgid = 0 表示插入的是 leaf page/bucket page
	key = cloneBytes(key)
	// 其中，c.node 操作会将 cursor 的遍历栈中保存的所有节点索引依次加载为 node，并缓存起来，避免重复加载以进行复用
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// Return an error if there is already existing bucket value.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// Sequence returns the current integer for the bucket without incrementing it.
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// SetSequence updates the sequence number for the bucket.
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence = v
	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stat returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				used += leafPageElementSize * int(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += used
			} else {
				// For non-inlined bucket update all the leaf stats
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		// Keep track of maximum page depth.
		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	s.Add(subStats)
	return s
}

// forEachPage iterates over every page in a bucket, including inline pages.
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// If we have an inline page then just use that.
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// Otherwise traverse the page hierarchy.
	b.tx.forEachPage(b.root, 0, fn)
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// If we have an inline page or root node then just use that.
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// Execute function.
	fn(p, n, depth)

	// Recursively loop over children.
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill writes all the nodes for this bucket to dirty pages.
// spill 将过大（尺寸大于一个 page）节点拆分、将节点写入脏页（dirty page）
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	// 先 spill 所有的子 bucket。
	// 针对每一个子 bucket，首先判断其是否可以被内联，若允许被内联，则先释放掉该子 bucket，然后构建内联 bucket。
	// 即直接将其内嵌到父 bucket 所在的 page 中，而不需要额外分配 page。
	// 否则，若不允许内联，则递归地先 spill subbucket，并更新其 bucket
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		var value []byte
		if child.inlineable() {
			child.free()
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// 然后将子 bucket 的根节点 page id 作为值写入父 bucket 相应叶子节点
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// Skip writing the bucket if there are no materialized nodes.
		// 若该子 bucket 没有缓存任何 node（说明没有数据变动），则直接跳过
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		// 更新 child 的 父 bucket（即本 bucket）的对该子 bucket 的引用
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	// 类似的，如果该 bucket 没有缓存任何 node（说明没有数据变动），则终止调整
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	// 调整本 bucket 的 root node
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	// 由于调整会增量写，造成本 bucket 根节点引用变更，因此需要更新 b.root
	b.root = b.rootNode.pgid

	return nil
}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
// inlineable 判断一个 bucket 是否足够小且不包含子 bucket 以使得我们可以直接将其内联，即直接内嵌在父 bucket 的叶子节点中
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	// 内联 bucket 必须只包含 leaf node
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	// 判断其包含的元素，是否都是 leaf node，且大小不能超过内联 bucket 的阈值，即 1/4 页大小
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// write allocates and writes a bucket to a byte slice.
// write 为当前 bucket 分配空间，并依次写入 bucket 头和一个 root page，最终将其包含的 root page 转换为 page
func (b *Bucket) write() []byte {
	// Allocate the appropriate size.
	// 计算并分配 bucket 头空间
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// Write a bucket header.
	// 写入 bucket 头
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// Convert byte slice to a fake page and write the root node.
	// 分配 bucket root page，并写入转换为 node
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// rebalance attempts to balance all nodes.
// 在写事务中，用户可能会进行一系列的新增/删除/修改操作，使得大量的相关节点被转化为 node 加载到内存中，
// 而改动后的 B+ 树由文件系统中的 page 和内存中的 node 共同构成。
// 且由于数据变动，可能会造成某些节点元素数量过少、而另外一些节点元素数量过多。
// 因此需要对这些节点进行（和相邻的左兄弟或者右兄弟）合并或者拆分操作。
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node creates a node from a page and associates it with a given parent.
// node 返回一个具有指定 page id 的新创建的或者已经存在的 node，并将其设置为指定 node 的子节点。
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// Retrieve node if it's already been created.
	// 若已经存在 bucket 中，则直接返回
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a node and cache it.
	// 否则，创建它，并指定它的父节点，将其加入到父节点的 children 集合中
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	// 对于 inline bucket 设置它的 inline page
	var p = b.page
	if p == nil { // 非内联桶，则通过事务获取对应的 page
		p = b.tx.page(pgid)
	}

	// Read the page into the node and cache it.
	// 同时将此 page 转换为 node，并缓存到 bucket 的子节点 缓存中
	n.read(p)
	b.nodes[pgid] = n

	// Update statistics.
	b.tx.stats.NodeCount++

	return n
}

// free recursively frees all pages in the bucket.
// free 依次递归释放当前 bucket 的所有 subbucket 和 leaf page
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0 // 设置 root 的 page id 为 0，表示当前 bucket 为内联 bucket
}

// dereference removes all references to the old mmap.
// dereference 即解引用 bucket 之前占用的内存空间。
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
// pageNode 返回指定 page id 所对应的 node 或者 page
// 该函数不会加载 page 为 node，而只是复用已经缓存的（bucket 缓存到以及 tx 缓存的） node ，或者直接访问 mmap 到内存空间中的相关 page
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// Inline buckets have a fake page embedded in their value so treat them
	// differently. We'll return the rootNode (if available) or the fake page.
	if b.root == 0 { // bucket 被内联，此时 bucket.root = 0。因此，需要直接返回内联的 root node，或者 root node 不存在则返回 page
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	// 否则，非内联的情况，返回 bucket 缓存中的指定 page id 的 node，若存在的话
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	// 否则，若 bucket 未缓存对应的 node，则只能去当前 bucket 关联的事务的 page 列表中查询
	// 其会去事务缓存的 pages（脏页） 去查询，若仍旧查询不到，则只能到事务所作用的 db 去查询，
	// 即从 db mmap 出来的内存数组中获取指定 page id 的 page 结构
	return b.tx.page(id), nil
}

// BucketStats records statistics about resources used by a bucket.
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
