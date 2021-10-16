package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
// freelist 作为空闲列表页 page 的 page body。
// 内存中空闲列表页结构。包含了真正空闲（未分配的）页列表，以及目前被某些事务使用但即将被释放的页列表。以及他们合并后的页缓存。
type freelist struct {
	ids     []pgid          // 所有空闲列表页的已排序的 id 列表
	   						// all free and available free page ids.
	pending map[txid][]pgid // 将来很快能被释放的空闲页，部分事务可能在读
							// mapping of soon-to-be free page ids by tx.
	cache   map[pgid]bool  // 缓存可用页和被事务正在读写的很快将被释放的页
							// fast lookup of all free and pending page ids.
}

// newFreelist returns an empty, initialized freelist.
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// size returns the size of the page after serialization.
// size 返回空闲列表结构在磁盘上的页数目，即 page id 的数目
// 若该页的数据段大小未超过 0xFFFF=65535 时，则即为 ids 的长度和 pending 的长度之和。
// 否则，count 的大小为 0xFFFF，实际大小存储在数据段的第一个元素中
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// free_count returns count of free pages
func (f *freelist) free_count() int {
	return len(f.ids)
}

// pending_count returns count of pending pages
func (f *freelist) pending_count() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

// copyall copies into dst a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
// copyall 将空闲列表页的 free 以及 pending 页列表合并排序后拷贝到目标数组中。
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
// allocate 返回连续分配的指定大小的页的首页id
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 { // 空闲页列表为空，则直接返回
		return 0
	}

	var initial, previd pgid
	// 遍历所有的空闲列表页（f.ids 已排序）
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		// 遍历空闲页列表时，若发现不连续的页，则此时重置记录的初始页号
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		// 若恰好发现一个连续的空闲页列表可供分配，则移除并返回它
		if (id-initial)+1 == pgid(n) { // 当前页id - 初始页id + 1 == 需要分配的页大小
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n { // 若此n各连续页恰好使前n个空闲页，则修改空闲页列表剩余的空闲页列表
				f.ids = f.ids[i+1:]
			} else { // 先拷贝分配之后的页到将要被移除的空闲块，即将需被分配出的空闲块抠出，将其后的往前拼接第一部分空闲块
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n] // 调整剩余空闲列表的空间（原有的减去已分配的）
			}

			// Remove from the free cache.
			// 刷新 f.cache 缓存页
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			// 最后将分配的空闲列表块的首页号返回
			return initial
		}

		// 继续更新当前记录的前一页id
		previd = id
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
// free 释放关联指定事务的 page
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	// 释放 page 及其 overflow page。overflow 字段即表示当前页 overflow 情况下的最大 page id
	var ids = f.pending[txid]
	// 遍历当前 page id 直到最大的 overflow page id
	// 若其已被释放，即已存在于 freelist 的 cache 列表中，则 panic；
	// 否则，将其添加到 freelist 的 cache 中，同时也加入到 freelist 的 pending 缓存中的指定事务关联的 page id 列表中。
	// 这里加入到 pending 缓存中表示该事务已释放该页，但是其他事务可能还引用当前页（实现 mvcc）因此不能直接释放。
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// Add to the freelist and cache.
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// Remove pages from pending list.
	delete(f.pending, txid)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// read initializes the freelist from a freelist page.
// read 将磁盘的页结构读取到内存转换为 freelist 空闲列表页结构
func (f *freelist) read(p *page) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	// 同 write 类似，若发现 page.count 为 0xFFFF，则页大小需要从 page.ptr 的第一个元素获取。
	idx, count := 0, int(p.count) // idx 表示数据读取的起始索引位置
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil // 空的空闲列表
	} else { // 读取 count 个空闲列表页，并将他们拷贝到 freelist 的 free ids 中。
		// 最后将空闲列表页的 free ids 进行了排序。
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		sort.Sort(pgids(f.ids))
	}

	// Rebuild the page cache.
	// 重建空闲列表页的缓存。
	f.reindex()
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
// write 将内存中的空闲列表页写到磁盘的 page 上。
// 但注意 write 操作并未将内存中的 freelist page 结构刷到磁盘上，该操作 fsync 实在事务提交时进行的。
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	// 设置页类型为空闲列表页
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	// 考虑到 freelist page 中的元素都是 pgid，没有 element header，没有各元素的位移信息，
	// 因此可能出现 freelist 的实际长度可能超过 count（uint16） 的上限 65535 的情况。
	// 若空闲列表页大小小于 64K，则直接存储在 p.count 字段。并将空闲页列表内容合并排序后拷贝到磁盘页 page 页结构的数据区
	// 否则，将 page 页结构的 count 设置为 0xFFFF，同时将实际的页大小存储在 page 页数据区的第一个元素中。
	// 然后同样将空闲页列表内容合并排序后拷贝到磁盘页 page 页结构的数据区从第1个元素后的区域。
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
// reload 同样从磁盘页 page 里面读取数据，但同时过滤掉 pending 页列表。
func (f *freelist) reload(p *page) {
	f.read(p) // 首先读取所有的页到 free ids 中。

	// Build a cache of only pending pages.
	// 为 pending 页列表构建缓存
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// 过滤出不在 pending 页缓存中的页列表，最后覆盖 free ids
	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// Once the available list is rebuilt then rebuild the free cache so that
	// it includes the available and pending free pages.
	// 同样刷新页缓存
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
// 根据 f.ids 和 f.pending 刷新 f.cache 缓存
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
