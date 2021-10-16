package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// 页头大小，16 字节
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

// 非页子节点大小
const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
// 页子节点大小
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))


// 各种类型的节点的标号
const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

// 磁盘页 page 结构
// page 由 page 头（前4个字段：8+2+2+4=16 字节） 以及 page 体构成
type page struct {
	id       pgid // 页 ID，8字节。id 相邻的 page 在物理存储上同样相邻，因此在 mmap 时，page_id * page_size 即为 page 在文件中存储的起始位置
	flags    uint16 // 页类型，2字节。包括中间节点（非叶子节点）、叶子节点、元信息节点以及空闲列表节点。
	count    uint16 // 点包含元素个数，2字节，因此最大 64K（65535）。用于统计非叶子节点包含的子页数、叶子节点包含的 kv 数目以及空闲列表页包含的页数。
	overflow uint32 // 数据页溢出个数，4字节。若页大于 4kb，则紧跟当前页后面就有多少个溢出的页。磁盘上连续的一个或多个页加载到内存中转换为一个二叉树的节点。
	ptr      uintptr // 真实存储数据的指针，即 page body 开始位置。TODO page 头和 page 体分开存储，非连续存储。该字段在磁盘文件中不存在，仅存在于内存中。
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	// 若该页为元数据页，则可将其转换为元数据页结构类型
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
// leafPageElement 获取 page 的指定索引出的 leaf page 的 kv 元素
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
// leafPageElements 返回该页包含的叶子节点列表
func (p *page) leafPageElements() []leafPageElement {

	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
// branchPageElement 返回指定索引位置的分支节点元素
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
// 分支元素。分支元素的集合构成非叶子节点，即分支节点。
// branch node 的 value 是子节点的 page id
type branchPageElement struct {
	pos   uint32 // 该 branchPageElement 对应的 key 在磁盘中的起始位置。因此 &key = &branchPageElement + pos
	ksize uint32 // key 的大小。因此 key = key[&key:&key+ksize:ksize]
	pgid  pgid // b+ 树中下一层的 page id，即指定 key 所在的 page，层层定位最终到 leaf page 查找该 key 对应的 value
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// leafPageElement represents a node on a leaf page.
// 叶子节点元素，包含了索引对应 kv 的元信息。
// 将 leaf page item header 和 item 分开存储减少了查找的时间
type leafPageElement struct {
	flags uint32 // 区分 subbucket 和普通 value，subbucket 为 bucketLeafFlag=0x01
	pos   uint32 // 实际存储 key 的位置同其对应的 leafPageElement 的距离
	ksize uint32 // key 大小，&key = &leafPageElement + pos
	vsize uint32 // value 大小，&val = &leafPageElement + pos + ksize
}

// key returns a byte slice of the node key.
// key 获取指定叶子节点元素的 key 的 byte 数组
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
// value 获取指定叶子节点元素的 key 关联的 value 的 byte 数组
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
// merge 返回页数组 a 和页数组 b 的合并后的页数组
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
// mergepgids 合并有序的 a 和 b 页列表到目标数组 dst 中。
// 且 a 和 b 的空闲列表总数不得超过 dst 的大小。
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		// 二分查找第一个 lead 数组中大于 follow 数组中元素的索引值，因此可将在则之前的数组拷贝到目标数组中
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		// 然后交换顺序，继续比较
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
