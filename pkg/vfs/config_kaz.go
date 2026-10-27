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

import "time"

// KazVfsConfig holds the VFS writer settings added by kaz. It is embedded at
// the end of Config without a JSON tag, so its fields keep their names and
// stay top-level keys of .config.
type KazVfsConfig struct {
	SliceFlushWait     time.Duration
	SliceFlushIdle     time.Duration
	WriterReuseWindow  int           // Distance before freezing nonmatching old slices; zero uses four.
	WriterFlushTimeout time.Duration // 0 waits without a deadline; AutoWriterFlushTimeout uses the legacy deadline.
	WriterFlushScope   string        // WriterFlushScopeFile (default) or WriterFlushScopeRange for Read/Fallocate barriers.
	MetaWriteBatch     int           // Maximum slices of one chunk committed per metadata transaction; 0 disables batching.
}
