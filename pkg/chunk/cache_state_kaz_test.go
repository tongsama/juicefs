/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chunk

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// kazStateTestConf returns a kaz-2q config for state file tests: no periodic
// rescan (the tests call scanCached themselves), no correlation period and a
// state interval long enough that the periodic saver never runs in a test.
func kazStateTestConf() Config {
	conf := defaultConf
	conf.CacheEviction = EvictionKaz2Q
	conf.KazCacheProbationRatio = 0.4
	conf.KazCachePromoteDelay = 0
	conf.KazCacheGhostRatio = 4
	conf.KazCacheStateInterval = time.Hour
	conf.CacheScanInterval = -1
	conf.FreeSpace = 0.01
	return conf
}

// newKazStateStore builds a real store with newCacheStore on dir (which must
// end with a separator, as cache.dir does). Like TestSetlimitByFreeRatio, the
// store goroutines are left running: shutdownStore writes s.state without the
// lock that available() lacks too, which -race reports.
func newKazStateStore(t *testing.T, dir string, conf Config) *cacheStore {
	t.Helper()
	return newCacheStore(newKazTestMetrics(), dir, 1<<30, 0, 1, &conf, nil)
}

// kazStateDir returns a fresh cache directory ending with a separator.
func kazStateDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "kaz-state-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir + string(filepath.Separator)
}

// kazGhostKeys returns the evicted-key list of the index, oldest first.
func kazGhostKeys(s *cacheStore, idx *kaz2QIndex) []cacheKey {
	s.Lock()
	defer s.Unlock()
	var keys []cacheKey
	for el := idx.ghost.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Value.(kazGhost).key)
	}
	return keys
}

// kazStateEvents returns the count of a 2Q event on the store metrics.
func kazStateEvents(s *cacheStore, name string) float64 {
	return testutil.ToFloat64(s.m.kaz2Q.events.WithLabelValues(name))
}

// TestKazCacheStateRoundTrip checks that export -> write -> read -> import
// gives back the same per-key state and evicted-key list.
func TestKazCacheStateRoundTrip(t *testing.T) {
	conf := kazStateTestConf()
	src := newKaz2QIndex(&conf)
	clock := kazTestStart
	src.now = func() uint32 { return clock }
	src.kazSetCapacity(1 << 30)
	k1 := cacheKey{id: 1, indx: 0, size: 1024}
	k2 := cacheKey{id: 2, indx: 3, size: 2048}
	k3 := cacheKey{id: 3, indx: 1, size: 512}
	src.add(k1, cacheItem{size: 1024, atime: clock - 100})
	src.add(k2, cacheItem{size: 2048, atime: clock - 50})
	src.add(cacheKey{id: 9, size: 4096}, cacheItem{size: -4096, atime: clock}) // staging: not saved
	clock += 10
	src.get(k1) // promoted (no correlation period)
	src.addGhost(k3, 512, kazPriorProbation)
	src.addGhost(cacheKey{id: 4, size: 100}, 100, kazPriorProbation)

	st := src.kazExportState()
	st.savedAt = clock
	path := filepath.Join(t.TempDir(), "kaz-cache-state.v1")
	require.NoError(t, writeKazCacheState(path, st))
	_, err := os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err), "temporary file must be renamed")
	got, err := readKazCacheState(path)
	require.NoError(t, err)
	require.Equal(t, st.savedAt, got.savedAt)
	require.ElementsMatch(t, st.items, got.items)
	require.Equal(t, st.ghosts, got.ghosts)
	require.Len(t, got.items, 2)

	dst := newKaz2QIndex(&conf)
	dst.kazSetCapacity(1 << 30)
	dst.kazImportState(got)
	require.True(t, dst.restoreFromFile)
	it1 := src.keys[k1]
	require.Equal(t, kazSaved{seg: kazProtected, atime: it1.atime, inserted: it1.inserted,
		order: it1.order, origin: it1.origin, hits: 1, read: true}, dst.restore[k1])
	require.Equal(t, kazProbation, dst.restore[k2].seg)
	require.Len(t, dst.restore, 2)
	var ghosts []cacheKey
	for el := dst.ghost.Front(); el != nil; el = el.Next() {
		ghosts = append(ghosts, el.Value.(kazGhost).key)
	}
	require.Equal(t, []cacheKey{k3, {id: 4, size: 100}}, ghosts)
	require.Equal(t, int64(612), dst.ghostBytes)
}

// TestKazCacheStateCorruptIgnored checks that a flipped byte, a truncated
// file, a wrong magic and an empty file are all rejected, and that a store
// still starts on such a file with every block in probation.
func TestKazCacheStateCorruptIgnored(t *testing.T) {
	conf := kazStateTestConf()
	src := newKaz2QIndex(&conf)
	src.kazSetCapacity(1 << 30)
	src.add(cacheKey{id: 1, size: 1024}, cacheItem{size: 1024, atime: 1})
	src.get(cacheKey{id: 1, size: 1024})
	src.addGhost(cacheKey{id: 2, size: 1024}, 1024, kazPriorProbation)
	good := filepath.Join(t.TempDir(), "good")
	require.NoError(t, writeKazCacheState(good, src.kazExportState()))
	data, err := os.ReadFile(good)
	require.NoError(t, err)

	flipped := append([]byte{}, data...)
	flipped[len(flipped)/2] ^= 0xff
	magic := append([]byte{}, data...)
	magic[0] = 'X'
	cases := map[string][]byte{
		"flipped":   flipped,
		"truncated": data[:len(data)-3],
		"magic":     magic,
		"empty":     {},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kaz-cache-state.v1")
			require.NoError(t, os.WriteFile(path, content, 0600))
			_, err := readKazCacheState(path)
			require.Error(t, err)

			dir := kazStateDir(t)
			s0 := newTestCacheStore(dir, &conf, nil)
			key := kazStoreKey(1)
			kazWriteBlock(t, s0, key)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "kaz-cache-state.v1"), content, 0600))
			s := newKazStateStore(t, dir, conf)
			require.Equal(t, float64(1), kazStateEvents(s, "state_load_failed"))
			s.scanCached(false)
			idx := s.keys.(*kaz2QIndex)
			require.Equal(t, kazProbation, kazSeg(t, s, idx, key))
			require.Empty(t, kazGhostKeys(s, idx))
		})
	}
}

// TestKazCacheStateRestart checks that a protected block and an evicted key
// saved by store A are restored by store B opened on the same directory.
func TestKazCacheStateRestart(t *testing.T) {
	conf := kazStateTestConf()
	dir := kazStateDir(t)
	a := newKazStateStore(t, dir, conf)
	a.scanCached(false)
	ia := a.keys.(*kaz2QIndex)
	prot, prob := kazStoreKey(1), kazStoreKey(2)
	now := uint32(time.Now().Unix())
	for _, k := range []string{prot, prob} {
		kazWriteBlock(t, a, k)
		a.add(k, kazStoreBlock, now)
	}
	kazLoad(t, a, prot)
	require.Equal(t, kazProtected, kazSeg(t, a, ia, prot))
	ghost := a.getCacheKey(kazStoreKey(3))
	a.Lock()
	ia.addGhost(ghost, kazStoreBlock, kazPriorProbation)
	a.Unlock()
	a.kazSaveState()
	require.Equal(t, float64(1), kazStateEvents(a, "state_saved"))
	_, err := os.Stat(a.kazStatePath())
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "kaz-cache-state.v1"), a.kazStatePath())

	b := newKazStateStore(t, dir, conf)
	ib := b.keys.(*kaz2QIndex)
	b.scanCached(false)
	require.Equal(t, kazProtected, kazSeg(t, b, ib, prot))
	require.Equal(t, kazProbation, kazSeg(t, b, ib, prob))
	require.Equal(t, []cacheKey{ghost}, kazGhostKeys(b, ib))
	require.Zero(t, kazStateEvents(b, "state_load_failed"))
}

// TestKazCacheStateIgnoredByLRU checks that an lru store opened on a
// directory with a state file sees all blocks and leaves the file untouched.
func TestKazCacheStateIgnoredByLRU(t *testing.T) {
	conf := kazStateTestConf()
	dir := kazStateDir(t)
	a := newKazStateStore(t, dir, conf)
	a.scanCached(false)
	keys := []string{kazStoreKey(1), kazStoreKey(2), kazStoreKey(3)}
	for _, k := range keys {
		kazWriteBlock(t, a, k)
		a.add(k, kazStoreBlock, uint32(time.Now().Unix()))
	}
	a.kazSaveState()
	path := a.kazStatePath()
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(path, old, old))

	lru := conf
	lru.CacheEviction = EvictionLRU
	s := newCacheStore(nil, dir, 1<<30, 0, 1, &lru, nil)
	s.scanCached(false)
	s.Lock()
	require.Equal(t, len(keys), s.keys.len())
	for _, k := range keys {
		require.NotZero(t, s.keys.peekAtime(s.getCacheKey(k)), "key %s", k)
	}
	s.Unlock()
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, fi.ModTime().Equal(old), "state file must not be touched")
}

// TestKazCacheStateStaleEntries checks that a saved key missing from raw/ is
// not in the index after a scan, and that a block in raw/ missing from the
// state file enters probation as legacy.
func TestKazCacheStateStaleEntries(t *testing.T) {
	conf := kazStateTestConf()
	dir := kazStateDir(t)
	s0 := newTestCacheStore(dir, &conf, nil)
	stale, fresh := kazStoreKey(1), kazStoreKey(2)
	kazWriteBlock(t, s0, fresh)
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(s0.cachePath(fresh), old, old))
	staleKey := s0.getCacheKey(stale)
	st := kazCacheState{savedAt: uint32(time.Now().Unix()), items: []kazStateItem{{key: staleKey,
		kazSaved: kazSaved{seg: kazProtected, atime: 10, inserted: 5, order: 5, origin: kazOriginFill, hits: 3}}}}
	require.NoError(t, writeKazCacheState(filepath.Join(dir, "kaz-cache-state.v1"), st))

	s := newKazStateStore(t, dir, conf)
	idx := s.keys.(*kaz2QIndex)
	s.scanCached(false)
	require.False(t, kazHas(s, idx, stale))
	require.Equal(t, kazProbation, kazSeg(t, s, idx, fresh))
	s.Lock()
	require.Equal(t, kazOriginLegacy, idx.keys[s.getCacheKey(fresh)].origin)
	s.Unlock()
}

// encodeKazCacheStateV1 builds a state file in the version 1 layout written by
// bf9efe73 (no demoted mark per item, no prior per evicted key), so that the
// tests can check that the current reader still restores it.
func encodeKazCacheStateV1(st kazCacheState) []byte {
	le := binary.LittleEndian
	var b []byte
	b = append(b, kazStateMagic...)
	b = le.AppendUint32(b, 1)
	b = le.AppendUint32(b, st.savedAt)
	b = le.AppendUint32(b, uint32(len(st.items)))
	for _, it := range st.items {
		b = le.AppendUint64(b, it.key.id)
		b = le.AppendUint32(b, it.key.indx)
		b = le.AppendUint32(b, uint32(int32(it.key.size)))
		b = append(b, byte(it.seg), byte(it.origin), it.hits)
		b = le.AppendUint32(b, it.atime)
		b = le.AppendUint32(b, it.inserted)
		b = le.AppendUint32(b, it.order)
	}
	b = le.AppendUint32(b, uint32(len(st.ghosts)))
	for _, g := range st.ghosts {
		b = le.AppendUint64(b, g.key.id)
		b = le.AppendUint32(b, g.key.indx)
		b = le.AppendUint32(b, g.key.size)
	}
	return le.AppendUint32(b, crc32.ChecksumIEEE(b))
}

// kazResum replaces the CRC32 trailer of an encoded state after a test edits it.
func kazResum(b []byte) []byte {
	body := b[:len(b)-4]
	return binary.LittleEndian.AppendUint32(append([]byte{}, body...), crc32.ChecksumIEEE(body))
}

// encodeKazCacheStateV2 builds a state file in the version 2 layout written
// by 57b3de8a (a demoted mark per item and a prior per evicted key, no read
// mark), so that the tests can check that the current reader still restores it.
func encodeKazCacheStateV2(st kazCacheState) []byte {
	le := binary.LittleEndian
	var b []byte
	b = append(b, kazStateMagic...)
	b = le.AppendUint32(b, 2)
	b = le.AppendUint32(b, st.savedAt)
	b = le.AppendUint32(b, uint32(len(st.items)))
	for _, it := range st.items {
		b = le.AppendUint64(b, it.key.id)
		b = le.AppendUint32(b, it.key.indx)
		b = le.AppendUint32(b, uint32(int32(it.key.size)))
		b = append(b, byte(it.seg), byte(it.origin), it.hits)
		b = le.AppendUint32(b, it.atime)
		b = le.AppendUint32(b, it.inserted)
		b = le.AppendUint32(b, it.order)
		var mark byte
		if it.demoted {
			mark = 1
		}
		b = append(b, mark)
	}
	b = le.AppendUint32(b, uint32(len(st.ghosts)))
	for _, g := range st.ghosts {
		b = le.AppendUint64(b, g.key.id)
		b = le.AppendUint32(b, g.key.indx)
		b = le.AppendUint32(b, g.key.size)
		b = append(b, byte(g.prior))
	}
	return le.AppendUint32(b, crc32.ChecksumIEEE(b))
}

// TestKazCacheStateV3RoundTrip checks that the demoted and read marks of items
// and the prior segment of evicted keys survive write -> read -> import ->
// export, that the file is written as version 3, and that out-of-range mark /
// prior bytes, a read mark on a probation item and an unknown version are
// rejected.
func TestKazCacheStateV3RoundTrip(t *testing.T) {
	conf := kazStateTestConf()
	k1 := cacheKey{id: 1, size: 1024}
	k2 := cacheKey{id: 2, size: 2048}
	k3 := cacheKey{id: 3, size: 4096}
	st := kazCacheState{savedAt: kazTestStart, items: []kazStateItem{
		{key: k1, kazSaved: kazSaved{seg: kazProbation, atime: 10, inserted: 11, order: 12, origin: kazOriginFill, hits: 2, demoted: true}},
		{key: k2, kazSaved: kazSaved{seg: kazProtected, atime: 20, inserted: 21, order: 22, origin: kazOriginCompaction, hits: 1}},
		{key: k3, kazSaved: kazSaved{seg: kazProtected, atime: 30, inserted: 31, order: 32, origin: kazOriginFill, hits: 0, read: true}},
	}}
	priors := []kazGhostPrior{kazPriorUnknown, kazPriorProbation, kazPriorDemoted, kazPriorProtected, kazPriorCompaction}
	for i, pr := range priors {
		k := cacheKey{id: uint64(10 + i), size: 100}
		st.ghosts = append(st.ghosts, kazGhost{key: k, size: 100, prior: pr})
	}
	data := encodeKazCacheState(st)
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(data[len(kazStateMagic):]))
	require.Len(t, data, kazStateHeader+4+3*kazStateItemSize+4+5*kazStateGhostSize+4)
	path := filepath.Join(t.TempDir(), "kaz-cache-state.v1")
	require.NoError(t, writeKazCacheState(path, st))
	got, err := readKazCacheState(path)
	require.NoError(t, err)
	require.Equal(t, st, got)

	dst := newKaz2QIndex(&conf)
	dst.kazSetCapacity(1 << 30)
	dst.kazImportState(got)
	require.True(t, dst.restore[k1].demoted)
	require.False(t, dst.restore[k2].demoted)
	require.False(t, dst.restore[k2].read, "the stored mark wins over the hit count")
	require.True(t, dst.restore[k3].read, "the stored mark wins over the hit count")
	dst.add(k1, cacheItem{size: 1024, atime: 1})
	require.True(t, dst.keys[k1].demoted)
	dst.add(k3, cacheItem{size: 4096, atime: 1})
	require.True(t, dst.keys[k3].read)
	for i, pr := range priors {
		require.Equal(t, pr, kazGhostPriorOf(t, dst, cacheKey{id: uint64(10 + i), size: 100}))
	}
	// the export carries the marks and the priors again
	exp := dst.kazExportState()
	require.ElementsMatch(t, st.items, exp.items)
	require.Equal(t, st.ghosts, exp.ghosts)

	item := kazStateHeader + 4 // first item (probation)
	ghostPrior := len(data) - 4 - 1
	bad := map[string][]byte{}
	b := append([]byte{}, data...)
	b[item+kazStateItemSize-2] = 2
	bad["demoted"] = kazResum(b)
	b = append([]byte{}, data...)
	b[item+2*kazStateItemSize-1] = 2
	bad["read"] = kazResum(b)
	b = append([]byte{}, data...)
	b[item+kazStateItemSize-1] = 1
	bad["read on probation"] = kazResum(b)
	b = append([]byte{}, data...)
	b[ghostPrior] = 5
	bad["prior"] = kazResum(b)
	b = append([]byte{}, data...)
	binary.LittleEndian.PutUint32(b[len(kazStateMagic):], 4)
	bad["version4"] = kazResum(b)
	b = append([]byte{}, data...)
	binary.LittleEndian.PutUint32(b[len(kazStateMagic):], 0)
	bad["version0"] = kazResum(b)
	bad["truncated"] = kazResum(data[:len(data)-1])
	for name, content := range bad {
		_, err := decodeKazCacheState(content)
		require.Error(t, err, name)
	}
}

// TestKazCacheStateReadsV2 checks that a version 2 file (as written by
// 57b3de8a, the running production version) is still restored: segments,
// demoted marks and priors are kept, and the read mark of a protected item is
// derived from its hit count (>= 1); probation items get no mark. A store that
// loads it writes version 3 on its next save.
func TestKazCacheStateReadsV2(t *testing.T) {
	conf := kazStateTestConf()
	k1 := cacheKey{id: 1, size: 1024}
	k2 := cacheKey{id: 2, size: 2048}
	k3 := cacheKey{id: 3, size: 4096}
	g1 := cacheKey{id: 7, size: 512}
	v2 := kazCacheState{savedAt: kazTestStart, items: []kazStateItem{
		{key: k1, kazSaved: kazSaved{seg: kazProtected, atime: 10, inserted: 11, order: 12, origin: kazOriginFill, hits: 2}},
		{key: k2, kazSaved: kazSaved{seg: kazProtected, atime: 20, inserted: 21, order: 22, origin: kazOriginCompaction, hits: 0}},
		{key: k3, kazSaved: kazSaved{seg: kazProbation, atime: 30, inserted: 31, order: 32, origin: kazOriginFill, hits: 5, demoted: true}},
	}, ghosts: []kazGhost{{key: g1, size: 512, prior: kazPriorDemoted}}}
	data := encodeKazCacheStateV2(v2)
	require.Len(t, data, kazStateHeader+4+3*kazStateV2ItemSize+4+kazStateGhostSize+4)
	got, err := decodeKazCacheState(data)
	require.NoError(t, err)
	want := v2
	want.items = append([]kazStateItem(nil), v2.items...)
	want.items[0].read = true // protected with hits >= 1
	require.Equal(t, want, got)
	_, err = decodeKazCacheState(kazResum(data[:len(data)-1]))
	require.Error(t, err)
	b := append([]byte{}, data...)
	b[kazStateHeader+4+kazStateV2ItemSize-1] = 2 // demoted mark of the first item
	_, err = decodeKazCacheState(kazResum(b))
	require.Error(t, err)

	p := newKaz2QIndex(&conf)
	p.kazSetCapacity(1 << 30)
	m := newKaz2QMetrics()
	p.kazSetMetrics(m)
	p.kazImportState(got)
	for _, k := range []cacheKey{k1, k2, k3} {
		p.add(k, cacheItem{size: int32(k.size), atime: 5})
	}
	require.True(t, p.keys[k1].read)
	require.False(t, p.keys[k2].read)
	require.False(t, p.keys[k3].read)
	require.True(t, p.keys[k3].demoted)
	require.Equal(t, int64(2048), p.usedUnread)
	require.Equal(t, 2048.0, testutil.ToFloat64(m.protectedUnread))
	require.Equal(t, kazPriorDemoted, kazGhostPriorOf(t, p, g1))

	// a store opened on a directory holding a version 2 file restores it,
	// and its next save writes version 3
	dir := kazStateDir(t)
	s0 := newTestCacheStore(dir, &conf, nil)
	prot := kazStoreKey(1)
	old := time.Now().Add(-time.Hour)
	kazWriteBlock(t, s0, prot)
	require.NoError(t, os.Chtimes(s0.cachePath(prot), old, old))
	saved := uint32(time.Now().Unix()) - 60
	file := kazCacheState{savedAt: saved, items: []kazStateItem{
		{key: s0.getCacheKey(prot), kazSaved: kazSaved{seg: kazProtected, atime: saved, inserted: saved - 10, order: saved - 10, origin: kazOriginFill, hits: 3}},
	}}
	require.NoError(t, os.WriteFile(filepath.Join(dir, kazStateFile), encodeKazCacheStateV2(file), 0600))
	s := newKazStateStore(t, dir, conf)
	idx := s.keys.(*kaz2QIndex)
	s.scanCached(false)
	require.Zero(t, kazStateEvents(s, "state_load_failed"))
	require.Equal(t, kazProtected, kazSeg(t, s, idx, prot))
	s.Lock()
	require.True(t, idx.keys[s.getCacheKey(prot)].read)
	s.Unlock()
	s.kazSaveState()
	b, err = os.ReadFile(s.kazStatePath())
	require.NoError(t, err)
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(b[len(kazStateMagic):]))
	st, err := decodeKazCacheState(b)
	require.NoError(t, err)
	require.Len(t, st.items, 1)
	require.True(t, st.items[0].read)
}

// TestKazCacheStateReadsV1 checks that a version 1 file (as written by the
// previous binary) is still restored: segments, atime and evicted keys are
// kept, items have no demoted mark and evicted keys an unknown prior, which a
// ghost hit counts as ghost_hit_probation; a protected item with hits >= 1
// gets the read mark.
func TestKazCacheStateReadsV1(t *testing.T) {
	conf := kazStateTestConf()
	k1 := cacheKey{id: 1, size: 1024}
	k2 := cacheKey{id: 2, indx: 3, size: 2048}
	g1 := cacheKey{id: 7, size: 512}
	v1 := kazCacheState{savedAt: kazTestStart, items: []kazStateItem{
		{key: k1, kazSaved: kazSaved{seg: kazProtected, atime: 10, inserted: 11, order: 12, origin: kazOriginFill, hits: 2}},
		{key: k2, kazSaved: kazSaved{seg: kazProbation, atime: 20, inserted: 21, order: 22, origin: kazOriginLegacy, hits: 0}},
	}, ghosts: []kazGhost{{key: g1, size: 512}}}
	data := encodeKazCacheStateV1(v1)
	require.Len(t, data, kazStateHeader+4+2*kazStateV1ItemSize+4+kazStateV1GhostSize+4)
	got, err := decodeKazCacheState(data)
	require.NoError(t, err)
	want := v1
	want.items = append([]kazStateItem(nil), v1.items...)
	want.items[0].read = true   // protected with hits >= 1
	require.Equal(t, want, got) // no demoted mark, prior 0
	// robustness checks hold for version 1 too
	_, err = decodeKazCacheState(kazResum(data[:len(data)-1]))
	require.Error(t, err)

	p := newKaz2QIndex(&conf)
	clock := kazTestStart
	p.now = func() uint32 { return clock }
	p.kazSetCapacity(1 << 30)
	m := newKaz2QMetrics()
	p.kazSetMetrics(m)
	p.kazImportState(got)
	p.add(k1, cacheItem{size: 1024, atime: 5})
	p.add(k2, cacheItem{size: 2048, atime: 5})
	require.Equal(t, kazProtected, p.keys[k1].seg)
	require.Equal(t, uint32(10), p.keys[k1].atime)
	require.False(t, p.keys[k1].demoted)
	require.True(t, p.keys[k1].read)
	require.False(t, p.keys[k2].read)
	require.Equal(t, kazProbation, p.keys[k2].seg)
	require.Equal(t, uint32(20), p.keys[k2].atime)
	require.Equal(t, kazPriorUnknown, kazGhostPriorOf(t, p, g1))
	p.add(g1, cacheItem{size: 512, atime: clock})
	require.Equal(t, kazProtected, p.keys[g1].seg)
	ev := func(name string) float64 { return testutil.ToFloat64(m.events.WithLabelValues(name)) }
	require.Equal(t, 1.0, ev("ghost_hit"))
	require.Equal(t, 1.0, ev("ghost_hit_probation"))
	require.Zero(t, ev("ghost_hit_demoted"))
	require.Zero(t, ev("ghost_hit_protected"))

	// a store opened on a directory holding a version 1 file restores it,
	// and its next save writes version 3
	dir := kazStateDir(t)
	s0 := newTestCacheStore(dir, &conf, nil)
	prot, prob := kazStoreKey(1), kazStoreKey(2)
	old := time.Now().Add(-time.Hour)
	for _, k := range []string{prot, prob} {
		kazWriteBlock(t, s0, k)
		require.NoError(t, os.Chtimes(s0.cachePath(k), old, old))
	}
	saved := uint32(time.Now().Unix()) - 60
	ghost := s0.getCacheKey(kazStoreKey(3))
	file := kazCacheState{savedAt: saved, items: []kazStateItem{
		{key: s0.getCacheKey(prot), kazSaved: kazSaved{seg: kazProtected, atime: saved, inserted: saved - 10, order: saved - 10, origin: kazOriginFill, hits: 3}},
		{key: s0.getCacheKey(prob), kazSaved: kazSaved{seg: kazProbation, atime: saved, inserted: saved, order: saved, origin: kazOriginFill, hits: 1}},
	}, ghosts: []kazGhost{{key: ghost, size: int64(ghost.size)}}}
	require.NoError(t, os.WriteFile(filepath.Join(dir, kazStateFile), encodeKazCacheStateV1(file), 0600))
	s := newKazStateStore(t, dir, conf)
	idx := s.keys.(*kaz2QIndex)
	s.scanCached(false)
	require.Zero(t, kazStateEvents(s, "state_load_failed"))
	require.Equal(t, kazProtected, kazSeg(t, s, idx, prot))
	require.Equal(t, kazProbation, kazSeg(t, s, idx, prob))
	require.Equal(t, saved, kazAtime(s, prot))
	require.Equal(t, []cacheKey{ghost}, kazGhostKeys(s, idx))
	s.kazSaveState()
	b, err := os.ReadFile(s.kazStatePath())
	require.NoError(t, err)
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(b[len(kazStateMagic):]))
}
