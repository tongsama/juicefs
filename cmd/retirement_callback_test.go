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

package cmd

import (
	"errors"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// retirementCallbackMeta records the callbacks installed by the actual mount adapter.
type retirementCallbackMeta struct {
	meta.Meta
	callbacks map[uint32]meta.MsgCallback
}

// OnMsg records registration without contacting metadata or starting a mount.
func (m *retirementCallbackMeta) OnMsg(kind uint32, callback meta.MsgCallback) {
	m.callbacks[kind] = callback
}

// retirementCallbackStore exercises the optional local-only store API independently of Remove.
type retirementCallbackStore struct {
	chunk.ChunkStore
	id      uint64
	length  int
	retired int
	removed int
	err     error
}

// Retire captures local-only requests and propagates a real adapter error.
func (s *retirementCallbackStore) Retire(id uint64, length int) error {
	s.id, s.length = id, length
	s.retired++
	return s.err
}

// Remove records physical deletion so the test detects accidental cloud work in retirement.
func (s *retirementCallbackStore) Remove(id uint64, length int) error {
	s.removed++
	return nil
}

// retirementLegacyStore deliberately lacks Retire to verify custom-store compatibility.
type retirementLegacyStore struct {
	chunk.ChunkStore
	removed int
}

// Remove preserves the original adapter contract for a store without local retirement.
func (s *retirementLegacyStore) Remove(id uint64, length int) error {
	s.removed++
	return nil
}

// TestRegisterMetaRetirement separates local retirement from remote removal and retains its errors.
func TestRegisterMetaRetirement(t *testing.T) {
	m := &retirementCallbackMeta{callbacks: make(map[uint32]meta.MsgCallback)}
	s := &retirementCallbackStore{}
	registerMetaMsg(m, s, &chunk.Config{})
	cb := m.callbacks[meta.RetireSlice]
	if cb == nil {
		t.Fatal("mount adapter does not register local retirement")
	}
	if err := cb(uint64(17), uint32(8192)); err != nil {
		t.Fatal(err)
	}
	if s.retired != 1 || s.removed != 0 || s.id != 17 || s.length != 8192 {
		t.Fatalf("local retirement called physical removal or lost identity: %+v", s)
	}
	want := errors.New("local retirement deferred")
	s.err = want
	if err := cb(uint64(17), uint32(8192)); !errors.Is(err, want) {
		t.Fatalf("retirement error lost: %v", err)
	}
	if err := m.callbacks[meta.DeleteSlice](uint64(17), uint32(8192)); err != nil || s.removed != 1 {
		t.Fatalf("physical removal changed: %v, %d", err, s.removed)
	}
}

// TestRegisterMetaRetirementLegacyStore keeps old custom stores usable through physical Remove.
func TestRegisterMetaRetirementLegacyStore(t *testing.T) {
	m := &retirementCallbackMeta{callbacks: make(map[uint32]meta.MsgCallback)}
	s := &retirementLegacyStore{}
	registerMetaMsg(m, s, &chunk.Config{})
	cb := m.callbacks[meta.RetireSlice]
	if cb == nil {
		t.Fatal("legacy store lacks a compatible retirement callback")
	}
	if err := cb(uint64(17), uint32(8192)); err != nil || s.removed != 0 {
		t.Fatalf("legacy retirement must defer cleanup to Remove: %v, %d", err, s.removed)
	}
	if err := m.callbacks[meta.DeleteSlice](uint64(17), uint32(8192)); err != nil || s.removed != 1 {
		t.Fatalf("legacy physical removal changed: %v, %d", err, s.removed)
	}
}
