package meta

import (
	"sync"
	"testing"

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
