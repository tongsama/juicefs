package meta

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// kazFakeStore stands in for the object store in block-compaction tests: slices
// written by a test hold deterministic bytes, slices built by compaction are kept.
type kazFakeStore struct {
	mu     sync.Mutex
	data   map[uint64][]byte
	calls  []uint64
	fail   func(id uint64) error // optional failure injected before a slice is stored
	during func()                // optional hook run once inside the first CompactChunk call
}

// newKazFakeStore returns an empty fake store.
func newKazFakeStore() *kazFakeStore {
	return &kazFakeStore{data: make(map[uint64][]byte)}
}

// kazFakeByte is the content of byte off of a test-written slice id (never zero).
func kazFakeByte(id uint64, off uint32) byte {
	return byte((id*131+uint64(off)*7)%251) + 1
}

// byteAtLocked returns byte off of slice id; holes (id 0) read as zero.
func (f *kazFakeStore) byteAtLocked(id uint64, off uint32) byte {
	if id == 0 {
		return 0
	}
	if d, ok := f.data[id]; ok {
		return d[off]
	}
	return kazFakeByte(id, off)
}

// compact is a CompactChunk callback: it materializes the pieces into slice id.
func (f *kazFakeStore) compact(args ...interface{}) error {
	pieces, id := args[0].([]Slice), args[1].(uint64)
	f.mu.Lock()
	hook, fail := f.during, f.fail
	f.during = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if fail != nil {
		if err := fail(id); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf []byte
	for _, p := range pieces {
		for i := uint32(0); i < p.Len; i++ {
			buf = append(buf, f.byteAtLocked(p.Id, p.Off+i))
		}
	}
	f.data[id] = buf
	f.calls = append(f.calls, id)
	return nil
}

// view returns what a reader sees in [0, n) of a chunk holding records ss.
func (f *kazFakeStore) view(ss []*slice, n uint32) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]byte, n)
	var pos uint32
	for _, p := range buildSlice(ss) {
		for i := uint32(0); i < p.Len && pos+i < n; i++ {
			out[pos+i] = f.byteAtLocked(p.Id, p.Off+i)
		}
		pos += p.Len
	}
	return out
}

// callCount returns how many slices compaction has written.
func (f *kazFakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// callIDs returns the ids of slices compaction has written.
func (f *kazFakeStore) callIDs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.calls...)
}

// applyKazPlan returns the records a chunk holds after plan p, writing each
// data output into f under a fresh id taken from next.
func applyKazPlan(t *testing.T, f *kazFakeStore, p *kazBlockPlan, next *uint64) []*slice {
	t.Helper()
	out := append([]*slice(nil), p.kept...)
	for _, o := range p.outs {
		if o.mask {
			out = append(out, &slice{pos: o.pos, len: o.length})
			continue
		}
		*next++
		require.NoError(t, f.compact(o.pieces, *next, uint8(0)))
		out = append(out, &slice{pos: o.pos, id: *next, size: o.length, len: o.length})
	}
	return out
}

// kazMarshal encodes records the way engines store a chunk.
func kazMarshal(ss []*slice) []byte {
	buf := make([]byte, 0, len(ss)*sliceBytes)
	for _, s := range ss {
		buf = append(buf, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	return buf
}

// kazIDs lists the slice ids of records in order.
func kazIDs(ss []*slice) []uint64 {
	ids := make([]uint64, len(ss))
	for i, s := range ss {
		ids[i] = s.id
	}
	return ids
}

// kazBlockBackends lists the engines tested; redis needs JUICEFS_TEST_COMPACTION_GC_REDIS.
var kazBlockBackends = []string{"memkv", "sqlite", "redis"}

// kazBlockEnv is a metadata backend wired for block-compaction tests.
type kazBlockEnv struct {
	gcTestBackend
	store   *kazFakeStore
	mu      sync.Mutex
	deleted map[uint64]int // DeleteSlice messages per slice id
}

// newKazBlockEnv returns a backend in block mode with the given block size, a fake
// object store and synchronous slice deletion that records DeleteSlice messages.
func newKazBlockEnv(t *testing.T, backend string, blockKiB int) *kazBlockEnv {
	t.Helper()
	b := newGCTestBackend(t, backend)
	b.Lock()
	b.fmt.BlockSize = blockKiB
	b.Unlock()
	b.conf.KazCompactionMode = KazCompactionModeBlock
	b.dSliceMu.Lock()
	b.dslices = nil // deleteSlice runs synchronously and sends DeleteSlice
	b.dSliceMu.Unlock()
	e := &kazBlockEnv{gcTestBackend: b, store: newKazFakeStore(), deleted: make(map[uint64]int)}
	b.OnMsg(CompactChunk, e.store.compact)
	b.OnMsg(DeleteSlice, func(args ...interface{}) error {
		e.mu.Lock()
		e.deleted[args[0].(uint64)]++
		e.mu.Unlock()
		return nil
	})
	return e
}

// file creates an empty regular file (subtest names contain "/", so they are sanitised).
func (e *kazBlockEnv) file(t *testing.T) Ino {
	t.Helper()
	var inode Ino
	var attr Attr
	require.Zero(t, e.Create(Background(), RootInode, strings.ReplaceAll(t.Name(), "/", "_"), 0600, 0, 0, &inode, &attr))
	return inode
}

// write appends a record of a fresh slice at chunk 0 offset pos, without commit hooks.
func (e *kazBlockEnv) write(t *testing.T, inode Ino, pos, length uint32) uint64 {
	t.Helper()
	var id uint64
	require.Zero(t, e.NewSlice(Background(), &id))
	var n int
	var d dirStat
	var attr Attr
	require.Zero(t, e.en.doWrite(Background(), inode, 0, pos, Slice{Id: id, Size: length, Len: length}, time.Now(), &n, &d, &attr))
	return id
}

// zero appends a zero record (as fallocate zero-range does) at chunk 0 offset pos.
func (e *kazBlockEnv) zero(t *testing.T, inode Ino, pos, length uint32) {
	t.Helper()
	var n int
	var d dirStat
	var attr Attr
	require.Zero(t, e.en.doWrite(Background(), inode, 0, pos, Slice{Len: length}, time.Now(), &n, &d, &attr))
}

// records returns the records of chunk 0.
func (e *kazBlockEnv) records(t *testing.T, inode Ino) []*slice {
	t.Helper()
	ss, st := e.en.doRead(Background(), inode, 0)
	require.Zero(t, st)
	return ss
}

// deletedIDs returns a copy of the DeleteSlice counts.
func (e *kazBlockEnv) deletedIDs() map[uint64]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[uint64]int, len(e.deleted))
	for k, v := range e.deleted {
		out[k] = v
	}
	return out
}
