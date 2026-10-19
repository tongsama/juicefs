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
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The kaz-2q state file keeps the 2Q segments and the evicted-key list of a
// cache store across restarts. It lives in cache.dir, outside raw/ and
// rawstaging/, so stores of other eviction policies and unmodified JuiceFS
// never see it. The file name keeps its ".v1" suffix; the layout version is
// the field after the magic. Layout (little endian):
//
//	magic "KAZ2QST1" | version u32 (1 or 2) | savedAt u32
//	items count u32, then per item:
//	  id u64 | indx u32 | size i32 | seg u8 | origin u8 | hits u8 | atime u32 | inserted u32 | order u32
//	  | demoted u8 (0 or 1; version 2 only)
//	ghosts count u32, then per evicted key, oldest first:
//	  id u64 | indx u32 | size u32 | prior u8 (0..4, kazGhostPrior; version 2 only)
//	CRC32 (IEEE) of everything before it, u32
//
// Version 2 is always written. Version 1 (written by bf9efe73) is still read:
// its items have no demoted mark and its evicted keys an unknown prior (0).
// Any other version is rejected, so the file is ignored.
const (
	kazStateFile        = "kaz-cache-state.v1"
	kazStateMagic       = "KAZ2QST1"
	kazStateVersion     = uint32(2)
	kazStateHeader      = len(kazStateMagic) + 4 + 4
	kazStateV1ItemSize  = 8 + 4 + 4 + 1 + 1 + 1 + 4 + 4 + 4
	kazStateV1GhostSize = 8 + 4 + 4
	kazStateItemSize    = kazStateV1ItemSize + 1  // + demoted
	kazStateGhostSize   = kazStateV1GhostSize + 1 // + prior
	kazFlushTimeout     = 30 * time.Second        // bound of the state flush at umount
)

// kazStateItem is the saved state of one cached block. key.size is the block
// size from the key (the item size of a cached block); staging blocks are not
// saved.
type kazStateItem struct {
	key cacheKey
	kazSaved
}

// kazCacheState is the content of the state file.
type kazCacheState struct {
	savedAt uint32
	items   []kazStateItem
	ghosts  []kazGhost // oldest first
}

// kazExportState returns the state to save: every cached (non-staging) block
// and, for keys a running (or never run) scan has not found yet, the restore
// state, plus the evicted-key list. It must run under the store mutex.
func (p *kaz2QIndex) kazExportState() kazCacheState {
	return p.kazExport(true)
}

// kazExport returns the state to save. withRestore adds restore entries not
// shadowed by a cached key; it is false once a scan has completed, when the
// remaining restore entries are keys no longer found in raw/.
func (p *kaz2QIndex) kazExport(withRestore bool) kazCacheState {
	st := kazCacheState{savedAt: p.now()}
	for k, it := range p.keys {
		if it.size <= 0 {
			continue
		}
		st.items = append(st.items, kazStateItem{key: k, kazSaved: kazSaved{seg: it.seg, atime: it.atime,
			inserted: it.inserted, order: it.order, origin: it.origin, hits: it.hits, demoted: it.demoted}})
	}
	if withRestore {
		for k, s := range p.restore {
			if _, ok := p.keys[k]; !ok {
				st.items = append(st.items, kazStateItem{key: k, kazSaved: s})
			}
		}
	}
	for el := p.ghost.Front(); el != nil; el = el.Next() {
		st.ghosts = append(st.ghosts, el.Value.(kazGhost))
	}
	return st
}

// kazImportState loads a state read from the file: items go into restore
// (marked as loaded from the file so that they survive one reset; cached keys
// win) and the evicted keys are appended to the evicted-key list as they are,
// trimmed to its cap. It must run under the store mutex.
func (p *kaz2QIndex) kazImportState(st kazCacheState) {
	for _, it := range st.items {
		if _, ok := p.keys[it.key]; ok {
			continue
		}
		p.restore[it.key] = it.kazSaved
	}
	p.restoreFromFile = true
	if p.ghostCap() > 0 {
		for _, g := range st.ghosts {
			if _, ok := p.ghostKeys[g.key]; ok {
				continue
			}
			if _, ok := p.keys[g.key]; ok {
				continue
			}
			p.ghostKeys[g.key] = p.ghost.PushBack(g)
			p.ghostBytes += g.size
		}
		p.trimGhost()
		p.syncGauges()
	}
}

// encodeKazCacheState serializes a state in the file layout, version 2.
func encodeKazCacheState(st kazCacheState) []byte {
	n := kazStateHeader + 4 + len(st.items)*kazStateItemSize + 4 + len(st.ghosts)*kazStateGhostSize + 4
	b := make([]byte, 0, n)
	le := binary.LittleEndian
	b = append(b, kazStateMagic...)
	b = le.AppendUint32(b, kazStateVersion)
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

// decodeKazCacheState parses the file layout, version 1 or 2 (the per-item and
// per-ghost sizes follow the version); any mismatch of magic, version,
// length, CRC or field range is an error.
func decodeKazCacheState(b []byte) (kazCacheState, error) {
	var st kazCacheState
	le := binary.LittleEndian
	if len(b) < kazStateHeader+4+4+4 {
		return st, fmt.Errorf("too short (%d bytes)", len(b))
	}
	if string(b[:len(kazStateMagic)]) != kazStateMagic {
		return st, errors.New("bad magic")
	}
	body := b[:len(b)-4]
	if sum := le.Uint32(b[len(b)-4:]); sum != crc32.ChecksumIEEE(body) {
		return st, errors.New("checksum mismatch")
	}
	off := len(kazStateMagic)
	version := le.Uint32(body[off:])
	itemSize, ghostSize := kazStateItemSize, kazStateGhostSize
	switch version {
	case 1:
		itemSize, ghostSize = kazStateV1ItemSize, kazStateV1GhostSize
	case kazStateVersion:
	default:
		return st, fmt.Errorf("unsupported version %d", version)
	}
	st.savedAt = le.Uint32(body[off+4:])
	off = kazStateHeader
	nItems := int(le.Uint32(body[off:]))
	off += 4
	if nItems > (len(body)-off)/itemSize {
		return st, fmt.Errorf("bad item count %d", nItems)
	}
	st.items = make([]kazStateItem, 0, nItems)
	for i := 0; i < nItems; i++ {
		e := body[off : off+itemSize]
		size := int32(le.Uint32(e[12:]))
		seg := kazSegment(e[16])
		if size <= 0 || (seg != kazProbation && seg != kazProtected) || kazOrigin(e[17]) > kazOriginCompaction {
			return st, fmt.Errorf("bad item %d", i)
		}
		var demoted bool
		if version >= 2 {
			if e[31] > 1 {
				return st, fmt.Errorf("bad item %d", i)
			}
			demoted = e[31] == 1
		}
		st.items = append(st.items, kazStateItem{
			key: cacheKey{id: le.Uint64(e), indx: le.Uint32(e[8:]), size: uint32(size)},
			kazSaved: kazSaved{seg: seg, origin: kazOrigin(e[17]), hits: e[18],
				atime: le.Uint32(e[19:]), inserted: le.Uint32(e[23:]), order: le.Uint32(e[27:]), demoted: demoted},
		})
		off += itemSize
	}
	if len(body)-off < 4 {
		return st, errors.New("missing ghost count")
	}
	nGhosts := int(le.Uint32(body[off:]))
	off += 4
	if nGhosts > (len(body)-off)/ghostSize || nGhosts*ghostSize != len(body)-off {
		return st, fmt.Errorf("bad ghost count %d", nGhosts)
	}
	st.ghosts = make([]kazGhost, 0, nGhosts)
	for i := 0; i < nGhosts; i++ {
		e := body[off : off+ghostSize]
		k := cacheKey{id: le.Uint64(e), indx: le.Uint32(e[8:]), size: le.Uint32(e[12:])}
		prior := kazPriorUnknown
		if version >= 2 {
			if prior = kazGhostPrior(e[16]); prior > kazPriorMax {
				return st, fmt.Errorf("bad evicted key %d", i)
			}
		}
		st.ghosts = append(st.ghosts, kazGhost{key: k, size: int64(k.size), prior: prior})
		off += ghostSize
	}
	return st, nil
}

// writeKazCacheState writes the state durably: <path>.tmp is written and
// fsynced, renamed to path, and the directory is fsynced.
func writeKazCacheState(path string, st kazCacheState) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(encodeKazCacheState(st))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

// readKazCacheState reads and validates the state file.
func readKazCacheState(path string) (kazCacheState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return kazCacheState{}, err
	}
	st, err := decodeKazCacheState(b)
	if err != nil {
		return st, fmt.Errorf("%s: %w", path, err)
	}
	return st, nil
}

// kazStatePath returns the path of the state file of this store.
func (cache *cacheStore) kazStatePath() string {
	return filepath.Join(cache.dir, kazStateFile)
}

// kazLoadState imports the state file if it exists. A missing file is normal;
// any other failure is logged and counted, and the store starts with every
// block in probation. It runs from kazAttach, before the first scan.
func (cache *cacheStore) kazLoadState(idx *kaz2QIndex) {
	path := cache.kazStatePath()
	st, err := readKazCacheState(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warnf("kaz-2q: ignore cache state file: %s", err)
			idx.event("state_load_failed")
		}
		return
	}
	cache.Lock()
	idx.kazImportState(st)
	cache.Unlock()
	logger.Infof("kaz-2q: loaded cache state %s: %d blocks, %d evicted keys", path, len(st.items), len(st.ghosts))
}

// kazSaveState exports the state under the store mutex and writes it outside
// of it. Failures are logged and counted only; they never affect cache
// operations. Saves of one store are serialized.
func (cache *cacheStore) kazSaveState() {
	idx, ok := cache.keys.(*kaz2QIndex)
	if !ok {
		return
	}
	idx.saveMu.Lock()
	defer idx.saveMu.Unlock()
	cache.Lock()
	st := idx.kazExport(!cache.scanned)
	cache.Unlock()
	if err := writeKazCacheState(cache.kazStatePath(), st); err != nil {
		logger.Warnf("kaz-2q: save cache state %s: %s", cache.kazStatePath(), err)
		cache.Lock()
		idx.event("state_save_failed")
		cache.Unlock()
		return
	}
	cache.Lock()
	idx.event("state_saved")
	cache.Unlock()
}

// kazSaveLoop saves the state every interval while the store is available.
func (cache *cacheStore) kazSaveLoop(interval time.Duration) {
	for cache.available() {
		time.Sleep(interval)
		if !cache.available() {
			return
		}
		cache.kazSaveState()
	}
}

// kazFlushState saves the state of every available kaz-2q store, in parallel,
// waiting at most kazFlushTimeout so that a hung disk cannot block umount.
func (m *cacheManager) kazFlushState() {
	m.Lock()
	stores := append([]*cacheStore(nil), m.stores...)
	m.Unlock()
	var wg sync.WaitGroup
	for _, s := range stores {
		if s == nil || !s.available() {
			continue
		}
		if _, ok := s.keys.(*kaz2QIndex); !ok {
			continue
		}
		wg.Add(1)
		go func(s *cacheStore) {
			defer wg.Done()
			s.kazSaveState()
		}(s)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(kazFlushTimeout):
		logger.Warnf("kaz-2q: saving cache state timed out after %s", kazFlushTimeout)
	}
}
