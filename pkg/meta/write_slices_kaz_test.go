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

package meta

import "testing"

// The production engines embed *baseMeta, so the vfs writer's type assertion
// to KazSliceWriter succeeds for them, and each keeps its one-transaction batch.
var (
	_ KazSliceWriter   = (*redisMeta)(nil)
	_ KazSliceWriter   = (*dbMeta)(nil)
	_ KazSliceWriter   = (*kvMeta)(nil)
	_ sliceBatchWriter = (*redisMeta)(nil)
	_ sliceBatchWriter = (*dbMeta)(nil)
	_ sliceBatchWriter = (*kvMeta)(nil)
)

// TestKazSliceWriterFromNewClient checks that the Meta returned by NewClient
// still offers WriteSlices after it left the Meta interface.
func TestKazSliceWriterFromNewClient(t *testing.T) {
	m := NewClient("memkv://", nil)
	if _, ok := m.(KazSliceWriter); !ok {
		t.Fatalf("%T does not implement KazSliceWriter", m)
	}
}
