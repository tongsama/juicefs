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

// fdatasync used to make staged writeback blocks durable (Linux).

import (
	"os"

	"golang.org/x/sys/unix"
)

// fdatasyncFile flushes file data and the size needed to read it back, skipping unrelated metadata such as mtime.
func fdatasyncFile(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
