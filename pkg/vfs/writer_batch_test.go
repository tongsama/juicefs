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

package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetaWriteBatchNormalization keeps valid sizes and disables invalid ones.
func TestMetaWriteBatchNormalization(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	for _, tc := range []struct{ in, want int }{{0, 0}, {1, 1}, {64, 64}, {1024, 1024}, {-1, 0}, {1025, 0}} {
		conf := *v.Conf
		conf.MetaWriteBatch = tc.in
		w := NewDataWriter(&conf, v.Meta, v.writer.(*dataWriter).store, v.reader).(*dataWriter)
		require.Equal(t, tc.want, w.conf.MetaWriteBatch, tc.in)
	}
}
