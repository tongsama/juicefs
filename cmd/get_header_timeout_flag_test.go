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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetHeaderTimeoutFlag checks that --kaz-get-header-timeout is off by default and parsed as a duration.
func TestGetHeaderTimeoutFlag(t *testing.T) {
	require.Zero(t, chunkConfFromArgs(t).GetHeaderTimeout, "must be disabled by default")
	require.Equal(t, 10*time.Second, chunkConfFromArgs(t, "--kaz-get-header-timeout=10s").GetHeaderTimeout)
	require.Equal(t, 8*time.Second, chunkConfFromArgs(t, "--kaz-get-header-timeout=8").GetHeaderTimeout)
}
