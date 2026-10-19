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

	"github.com/stretchr/testify/require"
)

// TestKaz2QCompatRoundTrip checks that a cache directory written by a kaz-2q
// store can be opened by an lru store without changing any managed file, and
// that blocks added and removed by the lru store are reflected correctly when
// kaz-2q opens the directory again: added blocks enter probation as legacy,
// removed blocks are absent, and the rest keep the segment saved earlier.
func TestKaz2QCompatRoundTrip(t *testing.T) {
	conf := kazStateTestConf()
	dir := kazStateDir(t)
	now := uint32(time.Now().Unix())

	// Phase 1: a kaz-2q store creates blocks (one protected, others in
	// probation) plus a staged block, then saves its state file.
	a := newKazStateStore(t, dir, conf)
	a.scanCached(false)
	ia := a.keys.(*kaz2QIndex)
	prot, probKeep, probDel := kazStoreKey(1), kazStoreKey(2), kazStoreKey(3)
	protDel := kazStoreKey(4)
	staged := kazStoreKey(5)
	for _, k := range []string{prot, probKeep, probDel, protDel} {
		kazWriteBlock(t, a, k)
		a.add(k, kazStoreBlock, now)
	}
	// a staged block is a raw/ file hard-linked from rawstaging/ (size < 0 in the index)
	kazWriteBlock(t, a, staged)
	require.NoError(t, os.MkdirAll(filepath.Dir(a.stagePath(staged)), 0700))
	require.NoError(t, os.Link(a.cachePath(staged), a.stagePath(staged)))
	a.add(staged, -kazStoreBlock, now)
	kazLoad(t, a, prot)
	kazLoad(t, a, protDel)
	require.Equal(t, kazProtected, kazSeg(t, a, ia, prot))
	require.Equal(t, kazProtected, kazSeg(t, a, ia, protDel))
	a.kazSaveState()
	require.Equal(t, float64(1), kazStateEvents(a, "state_saved"))

	statePath := a.kazStatePath()
	before, err := os.ReadFile(statePath)
	require.NoError(t, err)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(statePath, old, old))
	rawFiles := map[string][]byte{}
	for _, k := range []string{prot, probKeep, probDel, protDel, staged} {
		b, err := os.ReadFile(a.cachePath(k))
		require.NoError(t, err)
		rawFiles[k] = b
	}

	// Phase 2: an lru store opens the same directory and reads every block.
	lru := conf
	lru.CacheEviction = EvictionLRU
	l := newCacheStore(nil, dir, 1<<30, 0, 1, &lru, nil)
	l.scanCached(false)
	l.Lock()
	require.Equal(t, 5, l.keys.len())
	l.Unlock()
	for _, k := range []string{prot, probKeep, probDel, protDel, staged} {
		rc, err := l.load(k)
		require.NoError(t, err, "key %s", k)
		got := make([]byte, kazStoreBlock)
		n, _ := rc.ReadAt(got, 0)
		require.Equal(t, kazStoreBlock, n)
		require.Equal(t, rawFiles[k], got, "key %s", k)
		require.NoError(t, rc.Close())
	}

	// Phase 3: the lru store adds two blocks and removes two blocks (one that
	// kaz-2q saved as protected, one as probation).
	oldAtime := time.Now().Add(-24 * time.Hour)
	added1, added2 := kazStoreKey(6), kazStoreKey(7)
	for _, k := range []string{added1, added2} {
		kazWriteBlock(t, l, k)
		// an old atime makes the scan classify the block as legacy (not a recent fill)
		require.NoError(t, os.Chtimes(l.cachePath(k), oldAtime, oldAtime))
		l.add(k, kazStoreBlock, uint32(oldAtime.Unix()))
	}
	l.remove(probDel, false)
	l.remove(protDel, false)
	_, err = os.Stat(l.cachePath(probDel))
	require.True(t, os.IsNotExist(err))

	// The lru store never touched the kaz state file.
	after, err := os.ReadFile(statePath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	fi, err := os.Stat(statePath)
	require.NoError(t, err)
	require.True(t, fi.ModTime().Equal(old), "state file must not be touched by lru")

	// Phase 4: kaz-2q opens the directory again.
	b := newKazStateStore(t, dir, conf)
	ib := b.keys.(*kaz2QIndex)
	b.scanCached(false)
	require.Zero(t, kazStateEvents(b, "state_load_failed"))
	require.Equal(t, kazProtected, kazSeg(t, b, ib, prot), "saved protected block is restored")
	require.Equal(t, kazProbation, kazSeg(t, b, ib, probKeep), "saved probation block is restored")
	require.False(t, kazHas(b, ib, probDel), "block removed by lru must be absent")
	require.False(t, kazHas(b, ib, protDel), "protected block removed by lru must be absent")
	for _, k := range []string{added1, added2} {
		require.Equal(t, kazProbation, kazSeg(t, b, ib, k), "key %s", k)
		b.Lock()
		require.Equal(t, kazOriginLegacy, ib.keys[b.getCacheKey(k)].origin, "key %s", k)
		b.Unlock()
	}
	// the staged block is indexed as staging (size < 0)
	b.Lock()
	it := kazLookupNoTouch(b.keys, b.getCacheKey(staged))
	require.NotNil(t, it)
	require.Less(t, it.size, int32(0))
	b.Unlock()
}
