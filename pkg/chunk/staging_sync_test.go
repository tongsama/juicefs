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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stagingSyncRecorder records staging durability calls and the file state observed at each call.
type stagingSyncRecorder struct {
	fileSyncs       []string // path of the synced file
	finalAtFileSync []bool   // whether the final staging name already existed at file sync
	dirSyncs        []string
	fileErr         error
	dirErr          error
}

// install replaces the staging sync hooks for the duration of the test.
func (r *stagingSyncRecorder) install(t *testing.T, finalPath string) {
	origFile, origDir := syncStagingFile, syncStagingDir
	t.Cleanup(func() { syncStagingFile, syncStagingDir = origFile, origDir })
	syncStagingFile = func(f *os.File) error {
		r.fileSyncs = append(r.fileSyncs, f.Name())
		_, err := os.Stat(finalPath)
		r.finalAtFileSync = append(r.finalAtFileSync, err == nil)
		return r.fileErr
	}
	syncStagingDir = func(dir string) error {
		r.dirSyncs = append(r.dirSyncs, dir)
		return r.dirErr
	}
}

// newStagingSyncStore builds a writeback disk cache in a temp dir for staging durability tests.
func newStagingSyncStore(t *testing.T, noSync bool) *cacheStore {
	conf := defaultConf
	conf.Writeback = true
	conf.StagingNoSync = noSync
	conf.CacheScanInterval = -1
	m := new(cacheManagerMetrics)
	m.initMetrics()
	s := newCacheStore(m, t.TempDir(), 1<<30, conf.CacheItems, 1, &conf, nil)
	s.scanned = true
	return s
}

func TestStageSyncsFileBeforeRenameAndDirsAfter(t *testing.T) {
	s := newStagingSyncStore(t, false)
	key := "chunks/5/5974/5974010_0_4"
	final := s.stagePath(key)
	r := &stagingSyncRecorder{}
	r.install(t, final)

	path, err := s.stage(key, []byte("data"), 0)
	require.NoError(t, err)
	require.Equal(t, final, path)

	require.Equal(t, []string{final + ".tmp"}, r.fileSyncs, "staging data must be synced via the tmp file")
	require.Equal(t, []bool{false}, r.finalAtFileSync, "file sync must happen before the rename publishes the name")
	// The leaf dir holds the renamed entry; each newly created dir must also be durable in its parent.
	stagingRoot := filepath.Join(s.dir, stagingDir)
	require.Equal(t, []string{
		filepath.Join(stagingRoot, "chunks", "5", "5974"),
		filepath.Join(stagingRoot, "chunks", "5"),
		filepath.Join(stagingRoot, "chunks"),
		stagingRoot,
		s.dir, // rawstaging itself was created by this stage
	}, r.dirSyncs)

	// A second block in the existing dir only needs the leaf dir synced.
	r.dirSyncs = nil
	_, err = s.stage("chunks/5/5974/5974011_0_4", []byte("data"), 0)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(stagingRoot, "chunks", "5", "5974")}, r.dirSyncs)
}

func TestStageNoSyncSkipsDurabilityCalls(t *testing.T) {
	s := newStagingSyncStore(t, true)
	key := "chunks/5/5974/5974010_0_4"
	r := &stagingSyncRecorder{}
	r.install(t, s.stagePath(key))

	_, err := s.stage(key, []byte("data"), 0)
	require.NoError(t, err)
	require.Empty(t, r.fileSyncs)
	require.Empty(t, r.dirSyncs)
}

func TestCacheWriteDoesNotSync(t *testing.T) {
	s := newStagingSyncStore(t, false)
	key := "chunks/5/5974/5974010_0_4"
	r := &stagingSyncRecorder{}
	r.install(t, s.cachePath(key))

	s.cache(key, NewPage([]byte("data")), true, false)
	require.Eventually(t, func() bool {
		_, err := os.Stat(s.cachePath(key))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.Empty(t, r.fileSyncs, "read cache blocks are reproducible and must not pay for fsync")
	require.Empty(t, r.dirSyncs)
}

func TestStageFileSyncErrorFailsStage(t *testing.T) {
	s := newStagingSyncStore(t, false)
	key := "chunks/5/5974/5974010_0_4"
	final := s.stagePath(key)
	r := &stagingSyncRecorder{fileErr: errors.New("injected fdatasync error")}
	r.install(t, final)

	_, err := s.stage(key, []byte("data"), 0)
	require.ErrorIs(t, err, r.fileErr)
	_, statErr := os.Stat(final)
	require.True(t, os.IsNotExist(statErr), "an unsynced block must not be published")
	_, statErr = os.Stat(final + ".tmp")
	require.True(t, os.IsNotExist(statErr), "the tmp file must be cleaned up")
}

func TestStageDirSyncErrorFailsStage(t *testing.T) {
	s := newStagingSyncStore(t, false)
	key := "chunks/5/5974/5974010_0_4"
	r := &stagingSyncRecorder{dirErr: errors.New("injected dir fsync error")}
	r.install(t, s.stagePath(key))

	_, err := s.stage(key, []byte("data"), 0)
	require.ErrorIs(t, err, r.dirErr, "the caller must fall back to direct upload when the name is not durable")
}

func TestSyncHelpersOnRealFiles(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "f"))
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, fdatasyncFile(f))
	require.NoError(t, f.Close())
	require.NoError(t, fsyncDir(dir))
}

func TestStageSyncsAncestorsOfDirNotYetDurable(t *testing.T) {
	s := newStagingSyncStore(t, false)
	key := "chunks/5/5974/5974010_0_4"
	stagingRoot := filepath.Join(s.dir, stagingDir)
	leaf := filepath.Join(stagingRoot, "chunks", "5", "5974")
	// Another writer (or a previous process) created the dir, but nothing here has synced it into its parent yet.
	require.NoError(t, os.MkdirAll(leaf, 0755))
	r := &stagingSyncRecorder{}
	r.install(t, s.stagePath(key))

	_, err := s.stage(key, []byte("data"), 0)
	require.NoError(t, err)
	require.Equal(t, []string{
		leaf,
		filepath.Join(stagingRoot, "chunks", "5"),
		filepath.Join(stagingRoot, "chunks"),
		stagingRoot,
		s.dir,
	}, r.dirSyncs, "an existing dir is not known durable until this process syncs its parents")
}

func TestForgetDurableDirResyncsRecreatedDir(t *testing.T) {
	s := newStagingSyncStore(t, false)
	stagingRoot := filepath.Join(s.dir, stagingDir)
	leaf := filepath.Join(stagingRoot, "chunks", "5", "5974")
	r := &stagingSyncRecorder{}
	r.install(t, s.stagePath("chunks/5/5974/5974010_0_4"))
	_, err := s.stage("chunks/5/5974/5974010_0_4", []byte("data"), 0)
	require.NoError(t, err)

	// The staging scan removes an empty dir; a later stage recreates it and must sync it into its parent again.
	require.NoError(t, os.Remove(s.stagePath("chunks/5/5974/5974010_0_4")))
	require.NoError(t, os.Remove(leaf))
	s.forgetDurableDir(leaf)
	r.dirSyncs = nil
	_, err = s.stage("chunks/5/5974/5974011_0_4", []byte("data"), 0)
	require.NoError(t, err)
	require.Equal(t, []string{leaf, filepath.Join(stagingRoot, "chunks", "5")}, r.dirSyncs)
}
