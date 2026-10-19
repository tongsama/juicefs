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
	src.addGhost(k3, 512)
	src.addGhost(cacheKey{id: 4, size: 100}, 100)

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
		order: it1.order, origin: it1.origin, hits: 1}, dst.restore[k1])
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
	src.addGhost(cacheKey{id: 2, size: 1024}, 1024)
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
	ia.addGhost(ghost, kazStoreBlock)
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
