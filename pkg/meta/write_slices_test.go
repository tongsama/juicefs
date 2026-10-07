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

import (
	"testing"
)

// TestCompactionWanted matches the per-slice triggers for single writes and
// evaluates every count a batch passes through.
func TestCompactionWanted(t *testing.T) {
	for _, tc := range []struct {
		prev, now int
		want      bool
	}{
		{0, 1, false}, {98, 99, true}, {99, 100, false}, {198, 199, true},
		{350, 351, true}, {349, 350, false},
		// Batches: crossing a %100==99 count or exceeding 350 anywhere in (prev, now].
		{90, 105, true}, {100, 120, false}, {340, 360, true}, {0, 0, false},
	} {
		if got := compactionWanted(tc.prev, tc.now); got != tc.want {
			t.Errorf("compactionWanted(%d, %d) = %v, want %v", tc.prev, tc.now, got, tc.want)
		}
	}
	// Equivalent to the original single-write condition.
	for n := 1; n < 3000; n++ {
		if compactionWanted(n-1, n) != (n%100 == 99 || n > 350) {
			t.Fatalf("single write mismatch at %d", n)
		}
	}
}
