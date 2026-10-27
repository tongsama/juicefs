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
	"fmt"
	"sync"
	"testing"
)

// BenchmarkPriorityHintUpdate measures a hot-key update with a small or full bounded ready set.
func BenchmarkPriorityHintUpdate(b *testing.B) {
	for _, capacity := range []int{1, 1024} {
		b.Run(fmt.Sprint(capacity), func(b *testing.B) {
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			s := newCompactionScheduler(func(Ino, uint32, int) { once.Do(func() { close(started) }); <-release }, 1, capacity)
			defer func() { s.close(); s.wait() }()
			defer close(release)
			s.schedule(1, 0, 1000, 0)
			<-started
			for i := 1; i < capacity; i++ {
				s.schedule(Ino(i+1), 0, 100, 0)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.schedule(1, 0, 1000, 0)
			}
			b.StopTimer()
		})
	}
}
