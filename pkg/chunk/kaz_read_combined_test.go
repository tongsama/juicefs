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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestHeaderTimeoutAppliesToDetachedFetch checks that a GET kept running for a
// canceled reader (--kaz-finish-canceled-get) is still cut by --kaz-get-header-timeout
// instead of waiting for the shared 30s header timeout.
func TestHeaderTimeoutAppliesToDetachedFetch(t *testing.T) {
	store, gate, data := finishCanceledFixture(t, true, 4, 1)
	store.conf.GetHeaderTimeout = 200 * time.Millisecond
	key := blockKey(store, 1, len(data))

	// A reader that is not canceled gets the header-timeout error.
	r := waitResult(t, startRead(context.Background(), store, 1, len(data)))
	require.True(t, errors.Is(r.err, errGetHeaderTimeout), "unexpected error: %v", r.err)
	require.Equal(t, 1.0, testutil.ToFloat64(store.getHeaderTimeouts))
	waitStarted(t, gate, key) // consume the notification of that Get

	// A reader canceled while its GET waits for headers leaves a detached GET,
	// which must still give up at the header timeout. Forget the first cut so
	// that this GET is not treated as its retry.
	store.forgetHeaderTimeout(key)
	cctx, cancel := context.WithCancel(context.Background())
	res := startRead(cctx, store, 1, len(data))
	waitStarted(t, gate, key)
	cancel()
	require.True(t, errors.Is(waitResult(t, res).err, context.Canceled))
	require.Eventually(t, func() bool { return testutil.ToFloat64(store.getHeaderTimeouts) == 2 },
		2*time.Second, 10*time.Millisecond, "the detached GET must also give up at the header timeout")
}
