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
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// KazFlushCacheState saves the kaz-2q state file of every disk cache store
// (called at a normal umount). It does nothing for memory caches and other
// eviction policies; failures are only logged and counted.
func (store *cachedStore) KazFlushCacheState() {
	if m, ok := store.bcache.(interface{ kazFlushState() }); ok {
		m.kazFlushState()
	}
}

// kazCompactionReadKey is the context key that marks a read as made by
// compaction (see KazCompactionReadContext).
type kazCompactionReadKey struct{}

// KazCompactionReadContext marks ctx as a compaction read. Under kaz-2q such
// a read does not count as a user read: a cache hit neither updates atime nor
// promotes the block, and a miss is neither cached nor prefetched. Other
// eviction policies ignore the mark.
func KazCompactionReadContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, kazCompactionReadKey{}, true)
}

// kazCompactionRead reports whether ctx is marked as a compaction read and the
// store uses kaz-2q, so that lru/2-random/none behave exactly as before.
func (store *cachedStore) kazCompactionRead(ctx context.Context) bool {
	if store.conf.CacheEviction != EvictionKaz2Q {
		return false
	}
	marked, _ := ctx.Value(kazCompactionReadKey{}).(bool)
	return marked
}

// kazLoadForRead opens the cached block key for rSlice.ReadAt. A compaction
// read under kaz-2q uses the no-touch lookup when the block cache provides one;
// every other read uses bcache.load as before.
func (store *cachedStore) kazLoadForRead(ctx context.Context, key string) (ReadCloser, error) {
	if store.kazCompactionRead(ctx) {
		if m, ok := store.bcache.(interface {
			kazLoadNoTouch(key string) (ReadCloser, error)
		}); ok {
			return m.kazLoadNoTouch(key)
		}
	}
	return store.bcache.load(key)
}

// KazBlockRef names one block of a source slice of a compaction: the slice id,
// its full size (to build the block key) and the block index. Whole reports
// whether the full range of that source block was copied into the output
// block it is listed for (used by the replace placement). Bytes is the size
// of the union of the byte ranges of that source block copied into the output
// block, and OutBytes the non-hole bytes of the output block (the same in
// every ref of one output block); the share placement compares the Bytes of
// protected sources with OutBytes.
type KazBlockRef struct {
	ID        uint64
	SliceSize uint32
	Indx      uint32
	Whole     bool
	Bytes     uint32
	OutBytes  uint32
}

// KazSetCompactionSources marks the slice as a compaction output whose block
// indx is made from the source blocks f(indx). Under kaz-2q the synchronous
// upload then places each output block like its sources (see
// kazCacheCompactionOutput); under any other eviction policy it does nothing,
// so the upstream caching rule applies unchanged.
func (s *wSlice) KazSetCompactionSources(f func(indx int) []KazBlockRef) {
	if s.store.conf.CacheEviction != EvictionKaz2Q {
		return
	}
	s.kazSources = f
}

// kazCacheCompactionOutput replaces the upstream caching rule for a block of
// a compaction output under kaz-2q (it takes precedence over CacheLargeWrite
// and the small-block rule): the block is cached only when at least one of
// its source blocks is cached, with the newest user-read atime of the
// sources, in the segment chosen by --kaz-cache-compaction-placement (see
// cacheManager.kazCompactionPlacement). Like the upstream branch, cache() takes its own page
// reference and the caller releases block afterwards in both cases. A block
// cache without kaz placement (memory cache) keeps the upstream rule.
func (store *cachedStore) kazCacheCompactionOutput(key string, block *Page, sources func(int) []KazBlockRef) {
	placer, ok := store.bcache.(interface {
		kazCompactionPlacement(outKey string, outBytes uint32, srcs []kazPlacementSource) bool
	})
	if !ok {
		if len(block.Data) < store.conf.BlockSize || store.conf.CacheLargeWrite {
			store.bcache.cache(key, block, false, false)
		}
		return
	}
	// getCacheKey only parses the key, it does not use the store.
	indx := (*kazDiskCache)(nil).getCacheKey(key).indx
	refs := sources(int(indx))
	srcs := make([]kazPlacementSource, 0, len(refs))
	var outBytes uint32 // the same in every ref; max guards hand-made refs
	for _, ref := range refs {
		srcs = append(srcs, kazPlacementSource{
			key:   sliceForRead(ref.ID, int(ref.SliceSize), store).key(int(ref.Indx)),
			whole: ref.Whole,
			bytes: ref.Bytes,
		})
		outBytes = max(outBytes, ref.OutBytes)
	}
	if placer.kazCompactionPlacement(key, outBytes, srcs) {
		store.bcache.cache(key, block, false, false)
	}
}

// KazCacheState reports, for diagnostics and tests, whether the block key is
// in the local cache and its kaz-2q segment ("probation" or "protected"). It
// never counts as a read (no atime update, no promotion). A block that is
// still queued to be written to disk is reported as cached with segment ""
// (its segment is decided when it enters the index), so callers wait for a
// non-empty segment. Other eviction policies report segment "".
func (store *cachedStore) KazCacheState(key string) (segment string, cached bool) {
	if m, ok := store.bcache.(interface {
		kazCacheState(key string) (string, bool)
	}); ok {
		return m.kazCacheState(key)
	}
	_, cached = store.bcache.exist(key)
	return "", cached
}

// kazWholeRead reports whether a read under ctx of a block of bs bytes should
// fetch the whole object (and cache it) instead of a range, as configured by
// Config.KazWholeReadMax (delta compaction mode; 0 disables). It holds only
// when the fetched block is kept: not when the block is not cacheable
// (shouldCache) and not for a compaction read under kaz-2q, which is not
// cached; otherwise each small read would GET the whole object again.
// It only matters for an uncompressed volume: with lz4 or zstd the store is
// not seekable, so ReadAt never uses a range GET and always fetches whole.
func (store *cachedStore) kazWholeRead(ctx context.Context, bs int) bool {
	return store.conf.KazWholeReadMax > 0 && bs <= store.conf.KazWholeReadMax &&
		store.shouldCache(bs) && !store.kazCompactionRead(ctx)
}

// headerTimeoutMemory is how long a key cut by GetHeaderTimeout is remembered.
var headerTimeoutMemory = 5 * time.Minute

// maxHeaderTimedOut bounds the number of remembered keys.
const maxHeaderTimedOut = 4096

// headerTimedOutRecently reports whether key was cut by GetHeaderTimeout within
// headerTimeoutMemory, forgetting it once that has passed.
func (store *cachedStore) headerTimedOutRecently(key string) bool {
	store.headerTimedOutMu.Lock()
	defer store.headerTimedOutMu.Unlock()
	t, ok := store.headerTimedOut[key]
	if ok && time.Since(t) > headerTimeoutMemory {
		delete(store.headerTimedOut, key)
		return false
	}
	return ok
}

// rememberHeaderTimeout records that key was cut by GetHeaderTimeout.
// When too many keys are remembered, the expired ones are dropped first and,
// if that is not enough, all of them.
func (store *cachedStore) rememberHeaderTimeout(key string) {
	store.headerTimedOutMu.Lock()
	defer store.headerTimedOutMu.Unlock()
	if store.headerTimedOut == nil {
		store.headerTimedOut = make(map[string]time.Time)
	}
	if len(store.headerTimedOut) >= maxHeaderTimedOut {
		for k, t := range store.headerTimedOut {
			if time.Since(t) > headerTimeoutMemory {
				delete(store.headerTimedOut, k)
			}
		}
		if len(store.headerTimedOut) >= maxHeaderTimedOut {
			clear(store.headerTimedOut)
		}
	}
	store.headerTimedOut[key] = time.Now()
}

// forgetHeaderTimeout drops key once it has been downloaded.
func (store *cachedStore) forgetHeaderTimeout(key string) {
	store.headerTimedOutMu.Lock()
	defer store.headerTimedOutMu.Unlock()
	delete(store.headerTimedOut, key)
}

// errGetHeaderTimeout reports a full-block GET abandoned by GetHeaderTimeout.
var errGetHeaderTimeout = errors.New("no response headers within --kaz-get-header-timeout")

// readBlockDetached reads p from a full block fetched by a GET that is not tied to ctx.
// If ctx is canceled first, the caller returns at once while the GET goes on in the
// background and caches the block (see loadDetached). The fetch always uses its own
// page, so nothing writes into the caller's buffer after it has returned.
func (s *rSlice) readBlockDetached(ctx context.Context, p []byte, key string, boff, blockSize int) (int, error) {
	type result struct {
		block *Page
		err   error
	}
	done := make(chan result, 1)
	go func() {
		block, err := s.store.group.Execute(key, func() (*Page, error) {
			tmp := NewOffPage(blockSize)
			return tmp, s.store.loadDetached(ctx, key, tmp)
		})
		done <- result{block, err}
	}()
	select {
	case r := <-done:
		defer r.block.Release()
		if r.err != nil {
			return 0, r.err
		}
		copy(p, r.block.Data[boff:])
		return len(p), nil
	case <-ctx.Done():
		go func() { (<-done).block.Release() }()
		return 0, ctx.Err()
	}
}

// loadDetached downloads a full block into page and caches it, for readBlockDetached.
// It gives up while waiting for a download slot if ctx ends, because no request has
// been sent yet. Once the GET has started, a cancel of ctx no longer stops it, as long
// as fewer than detachedFetches GETs are already running this way; above that limit
// the GET is canceled as before. GetTimeout still applies.
func (store *cachedStore) loadDetached(ctx context.Context, key string, page *Page) error {
	select {
	case store.currentDownload <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-store.currentDownload }()

	gctx, gcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer gcancel()
	const (
		running = iota
		detached
		finished
	)
	var mu sync.Mutex
	state := running
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if state != running {
			return
		}
		select {
		case store.detachedFetches <- struct{}{}:
			state = detached
		default:
			store.canceledGetDropped.Inc()
			gcancel()
		}
	})
	defer stop()

	err := store.fetchBlock(gctx, key, page, true, false)

	mu.Lock()
	wasDetached := state == detached
	state = finished
	mu.Unlock()
	if wasDetached {
		<-store.detachedFetches
		if err == nil {
			store.canceledGetFinished.Inc()
			logger.Debugf("finished GET %s after its reader was canceled, block cached", key)
		}
	}
	return err
}

// cancelOnClose ends the request context of a GET when its body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

// Close closes the body and then releases its request context.
func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// getWithHeaderTimeout gets the whole object key, giving up with errGetHeaderTimeout
// when Get (which returns once the response headers arrive) takes longer than
// GetHeaderTimeout. After the headers, the body is read under ctx only, so a slow body
// is still bounded by GetTimeout alone. Abandoned GETs are retried by the caller's
// usual retry path.
//
// A key cut within headerTimeoutMemory is fetched again without the header timeout:
// some objects (e.g. on Google Drive) always take longer than it to answer, and
// cutting every retry would use up the read retries and fail the read with EIO.
func (store *cachedStore) getWithHeaderTimeout(ctx context.Context, key string, getters ...object.AttrGetter) (io.ReadCloser, error) {
	timeout := store.conf.GetHeaderTimeout
	if timeout <= 0 {
		return store.storage.Get(ctx, key, 0, -1, getters...)
	}
	if store.headerTimedOutRecently(key) {
		in, err := store.storage.Get(ctx, key, 0, -1, getters...)
		if err == nil {
			store.forgetHeaderTimeout(key)
		}
		return in, err
	}
	hctx, cancel := context.WithCancel(ctx)
	var fired atomic.Bool
	timer := time.AfterFunc(timeout, func() {
		fired.Store(true)
		cancel()
	})
	in, err := store.storage.Get(hctx, key, 0, -1, getters...)
	if !timer.Stop() && fired.Load() {
		if in != nil {
			_ = in.Close()
		}
		cancel()
		if ctx.Err() != nil { // the caller gave up first; report that instead
			return nil, ctx.Err()
		}
		store.getHeaderTimeouts.Inc()
		store.rememberHeaderTimeout(key)
		logger.Warnf("GET %s: %s (%s), will retry without it", key, errGetHeaderTimeout, timeout)
		return nil, fmt.Errorf("%w (%s)", errGetHeaderTimeout, timeout)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	return &cancelOnClose{in, cancel}, nil
}

// kazInitMetrics creates the kaz counters of the cached store; it is called at
// the end of initMetrics.
func (store *cachedStore) kazInitMetrics() {
	store.canceledGetFinished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kaz_canceled_get_finished",
		Help: "Full-block GETs finished and cached after their reader was canceled.",
	})
	store.canceledGetDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kaz_canceled_get_dropped",
		Help: "Full-block GETs aborted on cancel because too many were already kept running.",
	})
	store.getHeaderTimeouts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kaz_get_header_timeouts",
		Help: "Full-block GETs abandoned because no response headers arrived within --kaz-get-header-timeout.",
	})
}

// kazRegMetrics registers the kaz counters. It is called from regMetrics at the
// place where they were registered before, so the registration order is kept.
func (store *cachedStore) kazRegMetrics(reg prometheus.Registerer) {
	reg.MustRegister(store.canceledGetFinished)
	reg.MustRegister(store.canceledGetDropped)
	reg.MustRegister(store.getHeaderTimeouts)
}

// kazPutParam returns the param of the PUT request log: the payload length, which
// differs from the raw length in the key. It is empty unless the request is slow or
// debug logging is enabled. The string is the head of the logRequest param, and
// scripts that parse the log depend on it.
func (store *cachedStore) kazPutParam(payload int, used time.Duration) string {
	param := ""
	if used > SlowRequest || logger.IsLevelEnabled(logrus.DebugLevel) {
		param = fmt.Sprintf("payload_bytes=%d ", payload)
	}
	return param
}

// kazSliceKeys returns the object keys of every block of slice id with the given
// length, in block order. It replaces rSlice.keys(), which upstream removed.
func kazSliceKeys(id uint64, length int, store *cachedStore) []string {
	if length <= 0 {
		return nil
	}
	r := rSlice{id: id, length: length, store: store}
	lastIndx := (length - 1) / store.conf.BlockSize
	keys := make([]string, lastIndx+1)
	for i := 0; i <= lastIndx; i++ {
		keys[i] = r.key(i)
	}
	return keys
}
