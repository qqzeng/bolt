package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB // remmap 时的最大增量

// The data file format version.
const version = 2 // boltdb 当前版本

// Represents a marker value to indicate that a file is a Bolt DB.
const magic uint32 = 0xED0CDAED // 魔数

// IgnoreNoSync specifies whether the NoSync field of a DB is ignored when
// syncing changes to a file.  This is required as some operating systems,
// such as OpenBSD, do not have a unified buffer cache (UBC) and writes
// must be synchronized using the msync(2) syscall.
const IgnoreNoSync = runtime.GOOS == "openbsd" // 对于 openbsd，在将数据刷盘时 DB 的 NoSync 选项会被忽略

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

// default page size for db is set to the OS page size.
var defaultPageSize = os.Getpagesize() // 默认页大小同操作系统页保持一致，4KB

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
// DB 表示 bucket 的集合，也表示持久化到磁盘上的数据文件
type DB struct {
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	// 数据库严格模式，只在调试时使用，即其在每一次事务提交时都会执行 Check 动作以验证数据库数据的一致性
	StrictMode bool

	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	// NoSync 表示在事务提交时跳过刷盘的动作 fsync
	NoSync bool

	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	// NoGrowSync 表示在扩展数据库文件的大小时跳过 truncate 操作 TODO
	NoGrowSync bool

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	// MmapFlags 可设置为 syscall.MAP_POPULATE 以加速读取数据库磁盘文件到内存
	MmapFlags int

	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	// batch 事务的单个 batch 的大小，默认为 DefaultMaxBatchSize
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	// batch 事务的表示单个 batch 事务启动前的最大延迟
	MaxBatchDelay time.Duration

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	// AllocSize 表示当需要创建新的磁盘页时分配的空间大小，默认是 16MB
	AllocSize int

	// 数据库存储在磁盘上文件的位置
	path     string
	// 数据库文件
	file     *os.File
	lockfile *os.File // windows only
	dataref  []byte   // mmap'ed readonly, write throws SEGV
	// 磁盘文件 mmap 到内存后的数组
	data     *[maxMapSize]byte
	// 数组大小
	datasz   int
	// 磁盘文件大小
	filesz   int // current on disk file size
	// pageid = 0 和 1 的 元数据页
	meta0    *meta
	meta1    *meta
	// 页大小
	pageSize int
	// 数据库是否打开
	opened   bool
	// 打开的数据库关联的唯一的读写事务
	rwtx     *Tx
	// 关联的读事务的集合
	txs      []*Tx
	// pageid = 2 的空闲列表页
	freelist *freelist
	// 数据库操作统计信息
	stats    Stats

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	// 标记打开的数据库是否为只读的状态
	readOnly bool
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
// Passing in nil options will cause Bolt to open the database with the default options.
// Open 创建并打开指定路径的数据库文件，若该路径不存在，则创建它
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// 配置数据库选项
	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// Set default values for later DB operations.
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// Lock file so that other processes using Bolt in read-write mode cannot
	// use the database  at the same time. This would cause corruption since
	// the two processes would write meta pages and free pages separately.
	// The database file is locked exclusively (only one process can grab the lock)
	// if !options.ReadOnly.
	// The database file is locked using the shared lock (more than one process may
	// hold a lock at the same time) otherwise (options.ReadOnly is set).
	// 保证读写事务是严格串行执行的。只读事务不受影响，可以多个并发执行。
	// 获取操作系统的文件锁来保证事务的互斥性，若设置了 readOnly 标志，则获取共享锁，对应只读事务的情况。
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	// WriteAt 表示在指定文件的指定索引处写指定大小的字节内容
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 { // 数据库文件不存在，则执行 init 操作以创建它
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}
	} else { // 否则读取元数据页以初始化 db 结构
		// Read the first meta page to determine the page size.
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta() // 读取 meta 数据页
			if err := m.validate(); err != nil { // 验证 boltdb 魔数、版本已经校验和
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize() // 数据库创建所在的系统的页大小同当前加载数据库的系统的页大小不一致，导致解析元数据页错误
				// 因此需要更新页大小
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Initialize page pool.
	// 初始化 page pool，用于分配不超过一页大小的 page，可以提高分配效率。
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	// mmap 该 db 文件到内存中
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// Read in the freelist.
	// 初始化空闲列表页，即从 meta 页中解析出空闲列表页，然后通过 read 操作转换为内存中 db.freelist 结构
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// Mark the database as opened and return.
	return db, nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	// 根据数据库文件的大小计算需要 mmap 到内存的大小
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	// 解引用 db 之前占用的 mmap 的内存空间，以使得在 ummap 后其不会指向失效的内存空间
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	// ummap db 文件
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	// 再重新 mmap db 文件，mmap 完后，db.data 以及 db.datasz 会被更新
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	// 更新 db 的 2 个元数据页
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	// 校验两个元数据页，只要有一个校验成功即算校验成功，考虑到当一个校验失败时，即在落盘时出现问题，可以通过另一个元数据页进行恢复。
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
// mmapsize 计算将 db 文件 mmap 到内存所占用的内存空间大小。
// 1，基于最小值 32KB，每次翻倍直至 1GB；
// 2，确保申请的空间不大于最大空间 256TB（64位）；
// 3. 当申请的大小大于 1GB，则每次增加 1GB；
// 4. 确保申请的大小是 pagesize 对齐；
// 5. 再次确保申请的大小不能超过最大大小。
func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// init creates a new database file and initializes its meta pages.
// init 操作创建一个 db 文件，同时初始化两个 meta 页
func (db *DB) init() error {
	// Set the page size to the OS page size.
	// 设置 db 操作所使用的页大小与操作系统一致
	db.pageSize = os.Getpagesize()

	// Create two meta pages on a buffer.
	// 在 buffer 中创建两个 meta 页，第 0 页和第 1 页。同时初始化这两个 meta 页
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta() // 将内存 page 结构转化为 meta 页
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2 // freelist 为第 3 页
		m.root = bucket{root: 3} // 设置根节点的 page id
		m.pgid = 4 // 下一个待分配的页是 第 4 页
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// Write an empty freelist at page 3.
	// 在 pgid=2 的页写入一个空的空闲列表
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	// 在 pgid=3 的页写入一个空的叶子元素，也为当前的根节点
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// Write the buffer to our data file.
	// 将4个页的内容写入 db 文件，并刷盘
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
// 关闭事务时，必须释放所有的锁，
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	// 执行 mumap 操作
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	// 写事务释放 flock
	if db.file != nil {
		// No need to unlock read-only file.
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.

// Begin 用于开启一个事务，通过 writable 参数来指定该事务是否为读写事务，还是只读事务。
// 1. boltdb 允许多个只读事务同时存在，但同一时刻只允许一个写事务执行，即若同时开启多个写事务也会导致他们串行执行。
// 2. boltdb 事务之间不能相互依赖，如果在同一个 goroutine 中打开一个读事务和一个写事务，会导致写事务死锁，
// 因为写事务需要周期性地 re-mmap 当前数据库文件，但由于当前存在读事务，因此，该操作不能成功。
// 3. 如果需要执行一个长时间运行的只读事务，则建议将 mmap 的初始大小调大，以避免写事务被阻塞。TODO
// 4. 在只读事务执行完毕后，注意必须关闭它，否则，其引用的 page 不会被 boltdb 回收。
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

// beginTx 开启读写事务
func (db *DB) beginTx() (*Tx, error) {
	// Lock the meta pages while we initialize the transaction. We obtain
	// the meta lock before the mmap lock because that's the order that the
	// write transaction will obtain them.
	db.metalock.Lock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	// 创建一个事务，同时初始化其关联的 db 实例
	t := &Tx{}
	t.init(db)

	// Keep track of transaction until it closes.
	// 保存当前打开的事务
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Update the transaction stats.
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

// beginRWTx 开启读写事务
func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	// rwlock 保证一次只执行一个写事务，在事务 close 时会释放掉该锁。
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	// 类似地，创建一个写事务，同时初始化其关联的 db 实例
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
	// 释放掉已关闭的读事务关联的 page
	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	db.metalock.Lock()

	// Remove the transaction.
	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Merge statistics.
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
//
// Update 在读写事务环境中执行一个更新过程。
// 如果该过程未返回错误，则该更新过程会被提交，否额，该更新过程会被回滚。
// 不允许客户端显式调用 commit 或者 rollback 操作，会导致 panic。
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	// defer 调用 rollback，以保证在 panic 时也会回滚事务。
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	// 设置 managed 为 true
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.

// View 在只读事务环境中执行一个只读过程。同样无需用户手动 commit 和 rollback
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error { return fdatasync(db) }

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// This is for internal access to the raw data bytes from the C cursor, use
// carefully, or not at all.
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page retrieves a page reference from the mmap based on the current page size.
// page 从 db mmap 出来的内存数组中获取指定 page id 的 page 结构
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
// pageInBuffer 从指定字节数组中获取指定 page id 的页内容并转换为 page 结构
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	// 设置初始的 mmap 的大小
	InitialMmapSize int
}

// DefaultOptions represent the options used if nil options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats represents statistics about the database.
type Stats struct {
	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr
	PageSize int
}

// boltdb 数据存储的元信息结构。
// 它告诉我们文件是什么，同时如何来理解整个数据库文件
// meta page 的结构是作为 page 体的一部分。
// meta page 只有两页，其 id 要么为 0，要么为 1。
type meta struct {
	magic    uint32 // 魔数，固定为 0xED0CDAED。另一种类型的文件在文件起始位置具有相同数据的可能性极低
	version  uint32 // boltdb 版本，目前为 2。
	pageSize uint32 // 页大小，和操作系统的页保持一致
	flags    uint32 // 保留字段
	root     bucket // 根节点的 page id
	freelist pgid // 空闲列表页 id
	pgid     pgid // 下一个要分配的 page id
	txid     txid // 下一个要分配的事务 id
	checksum uint64 // 校验和，用于验证 meta page 页本身的完整性
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
// 验证元数据页合法必须 魔数+version+checksum 都正确才行
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
